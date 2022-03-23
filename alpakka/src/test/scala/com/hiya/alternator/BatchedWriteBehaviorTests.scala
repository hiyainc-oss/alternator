package com.hiya.alternator

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.hiya.alternator.alpakka._
import com.hiya.alternator.alpakka.stream._
import com.hiya.alternator.testkit.{DynamoDBLossyClient, LocalDynamoDB, Timeout => TestTimeout}
import com.hiya.alternator.util._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random


class BatchedWriteBehaviorTests extends AnyFunSpec with Matchers with Inside with BeforeAndAfterAll with Inspectors {

  private implicit val system = ActorSystem()
  private implicit val ec = system.dispatcher

  private val TEST_TIMEOUT = 20.seconds

  private val stableClient = LocalDynamoDB.client(Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(8484))
  private val lossyClient: DynamoDbAsyncClient = new DynamoDBLossyClient(stableClient)

  private val retryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy(
    awsHasRetry = false,
    maxRetries = 100,
    throttleBackoff = BackoffStrategy.FullJitter(1.second, 20.millis)
  )

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), 60.seconds)
    super.afterAll()
  }
  private implicit val timeout: Timeout = 60.seconds
  private implicit val testTimeout: TestTimeout = 60.seconds

  private implicit val scheduler: Scheduler = system.scheduler.toTyped

  implicit val reader: ActorRef[BatchedReadBehavior.BatchedRequest] =
    system.spawn(alpakka.BatchedReadBehavior(stableClient, 10.millis, retryPolicy), "reader")
  implicit val writer: ActorRef[BatchedWriteBehavior.BatchedRequest] =
    system.spawn(alpakka.BatchedWriteBehavior(lossyClient, 10.millis, retryPolicy), "writer")

  def streamWrite[Data](implicit tableConfig: TableConfig[Data]): Unit = {
    def generateData(nums: Int, writes: Int): List[Data] = {
      val state = (0 until nums).map {
        _ -> 0
      }.toArray
      var stateSize = state.length
      val result = List.newBuilder[Data]

      while (stateSize > 0) {
        val i = Random.nextInt(stateSize)
        val (idx, len) = state(i)

        if (len == writes - 1) {
          result += tableConfig.createData(idx)._2
          stateSize -= 1
          state(i) = state(stateSize)
        } else {
          result += tableConfig.createData(idx, Some(idx + Random.nextInt(100)))._2
          state(i) = idx -> (len + 1)
        }
      }

      result.result()
    }

    it("should report if table not exists") {
      val table = tableConfig.table("doesnotexists", stableClient)

      intercept[ResourceNotFoundException] {
        Await.result(
          Source(List(1))
            .map(k => tableConfig.createData(k))
            .map { case (k, _) => toStreamOps(table.table).deleteRequest(k) }
            .via(Alpakka.unorderedWriter(100))
            .grouped(Int.MaxValue)
            .runWith(Sink.head),
          TEST_TIMEOUT)
      }
    }


    it("should write data") {
      val nums = 100
      val writes = 10

      val (result, data) = tableConfig.withTable(stableClient) { table =>
        val result = Await.result({
          val q = generateData(nums, writes)
          Source(q)
            .mapAsync(100)(table.batchedPut)
            .grouped(Int.MaxValue)
            .runWith(Sink.head)
        }, 1.minute)

        val data = Await.result({
          Source(0 until nums)
            .map(k => tableConfig.createData(k))
            .via(table.batchedGetFlowUnordered[Data](100))
            .grouped(Int.MaxValue)
            .runWith(Sink.head)
        }, TEST_TIMEOUT)

        result -> data
      }

      result.size shouldBe nums * writes
      data.size shouldBe nums

      forAll(data) { case (data, pt) =>
        inside(data) {
          case Some(Right(p)) => p shouldBe pt
        }
      }
    }

    it("should stop") {
      val writer: ActorRef[BatchedWriteBehavior.BatchedRequest] =
        system.spawn(alpakka.BatchedWriteBehavior(lossyClient, 10.millis, retryPolicy), "writer2")

      Await.result(writer.terminate(), 10.seconds)

    }

  }

  describe("stream with PK table") {
    it should behave like streamWrite[DataPK]
  }

  describe("stream with RK table") {
    it should behave like streamWrite[DataRK]
  }

}
