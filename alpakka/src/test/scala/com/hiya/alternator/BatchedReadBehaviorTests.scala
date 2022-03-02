package com.hiya.alternator

import akka.Done
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.hiya.alternator.Table.PK
import com.hiya.alternator.syntax._
import com.hiya.alternator.testkit.{DynamoDBLossyClient, LocalDynamoDB}
import com.hiya.alternator.testkit.{Timeout => TestTimeout}
import com.hiya.alternator.util._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random


class BatchedReadBehaviorTests extends AnyFunSpec with Matchers with Inside with BeforeAndAfterAll with Inspectors {

  private implicit val system = ActorSystem()
  private implicit val ec = system.dispatcher
  private val TEST_TIMEOUT = 20.seconds

  private val stableClient = LocalDynamoDB.client()
  private val lossyClient: DynamoDbAsyncClient = new DynamoDBLossyClient(stableClient)

  override protected def afterAll(): Unit = {
    Await.result(reader.terminate(), 10.seconds)
    Await.result(writer.terminate(), 10.seconds)
    Await.result(system.terminate(), 60.seconds)
    super.afterAll()
  }

  private implicit val timeout: Timeout = 60.seconds
  private implicit val testTimeout: TestTimeout = 60.seconds
  private implicit val scheduler: Scheduler = system.scheduler.toTyped

  private val retryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy(
    awsHasRetry = false,
    maxRetries = 100,
    throttleBackoff = BackoffStrategy.FullJitter(1.second, 20.millis)
  )

  object monitoring extends BatchMonitoring {
    private var inflightF: () => Int = _
    private var queueSizeF: () => Int = _
    var retries = 0
    var requests = 0

    def inflight(): Int = inflightF()
    def queueSize(): Int = queueSizeF()


    override def register(actorName: String, behavior: BatchedBehavior): Unit = {
      inflightF = () => behavior.inflight
      queueSizeF = () => behavior.queueSize
    }

    override def retries(actorName: String, failed: List[PK]): Unit = ()
    override def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit = ()
    override def close(): Unit = {}
  }

  implicit val reader: ActorRef[BatchedReadBehavior.BatchedRequest] =
    system.spawn(BatchedReadBehavior(lossyClient, 10.millis, retryPolicy, monitoring), "reader")

  implicit val writer: ActorRef[BatchedWriteBehavior.BatchedRequest] =
    system.spawn(BatchedWriteBehavior(stableClient, 10.millis, retryPolicy), "writer")


  def streamRead[Data](implicit tableConfig: TableConfig[Data]): Unit = {
    def writeData(table: Table[Data, tableConfig.Key], nums: immutable.Iterable[Int]): Future[Seq[Done]] = {
      Source(nums)
        .map(v => tableConfig.createData(v)._2)
        .mapAsync(10)(table.batchedPut)
        .grouped(Int.MaxValue)
        .runWith(Sink.head)
    }

    def withData[T](nums: immutable.Iterable[Int])(f: Table[Data, tableConfig.Key] => T): T = {
      tableConfig.withTable(stableClient) { table =>
        Await.result(writeData(table, nums), TEST_TIMEOUT)
        f(table)
      }
    }

    it("should report if table not exists") {
      val table: Table[Data, tableConfig.Key] =
        tableConfig.table("doesnotexists")

      intercept[ResourceNotFoundException] {
        Await.result(
          Source(List(1))
            .map(k => tableConfig.createData(k))
            .via(table.batchedGetFlowUnordered[Data](100))
            .grouped(Int.MaxValue)
            .runWith(Sink.head),
          TEST_TIMEOUT)
      }

      monitoring.inflight() shouldBe 0
      monitoring.queueSize() shouldBe 0
    }

    it("should read data") {
      val result = withData(1 to 100) { table =>
        Await.result(
          Source(Random.shuffle(List.fill(10)(1 to 100).flatten))
            .map(k => tableConfig.createData(k))
            .via(table.batchedGetFlowUnordered[Data](100))
            .grouped(Int.MaxValue)
            .runWith(Sink.head),
          TEST_TIMEOUT)
      }

      result.size shouldBe 1000
      monitoring.inflight() shouldBe 0
      monitoring.queueSize() shouldBe 0

      forAll (result) { case (data, pt) =>
        inside(data) {
          case Some(Right(p)) => p shouldBe pt
        }
      }
    }

    it("should read empty table") {

      val result = tableConfig.withTable(stableClient) { table =>
        Await.result(
          Source(Random.shuffle(List.fill(10)(1 to 100).flatten))
            .map(k => tableConfig.createData(k))
            .via(table.batchedGetFlowUnordered[Data](100))
            .grouped(Int.MaxValue)
            .runWith(Sink.head),
          TEST_TIMEOUT)
      }

      result.size shouldBe 1000
      monitoring.inflight() shouldBe 0
      monitoring.queueSize() shouldBe 0

      forAll (result) { case (data, _) =>
        inside(data) {
          case None =>
        }
      }
    }

    it("should stop") {
      val reader: ActorRef[BatchedReadBehavior.BatchedRequest] =
        system.spawn(BatchedReadBehavior(lossyClient, 10.millis, retryPolicy, monitoring), "reader2")

      Await.result(reader.terminate(), 10.seconds)

    }
  }

  describe("stream with PK table") {
    it should behave like streamRead[DataPK]
  }

  describe("stream with RK table") {
    it should behave like streamRead[DataRK]

  }

}
