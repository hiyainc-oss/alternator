package com.hiya.alternator

import akka.Done
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.hiya.alternator.util._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random


class BatchedReadBehaviorTests extends AnyFunSpec with Matchers with Inside with BeforeAndAfterAll with Inspectors {

  private implicit val system = ActorSystem()

  private val TEST_TIMEOUT = 20.seconds

  private val stableClient = LocalDynamoDB.client(Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(8484))
  private val lossyClient: DynamoDbAsyncClient = new DynamoDbLossyClient(stableClient)

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), 60.seconds)
    super.afterAll()
  }

  private implicit val timeout: Timeout = 60.seconds
  private implicit val scheduler: Scheduler = system.scheduler.toTyped

  implicit val reader: ActorRef[BatchedReadBehavior.BatchedRequest] =
    system.spawn(BatchedReadBehavior(lossyClient, 10.millis, (_: Int) => 10.millis), "reader")

  implicit val writer: ActorRef[BatchedWriteBehavior.BatchedRequest] =
    system.spawn(BatchedWriteBehavior(stableClient, 10.millis, (_: Int) => 10.millis), "writer")


  def streamWrite[Data](implicit tableConfig: TableConfig[Data]): Unit = {
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

      forAll (result) { case (data, _) =>
        inside(data) {
          case None =>
        }
      }
    }
  }

  describe("stream with PK table") {
    it should behave like streamWrite[DataPK]
  }

  describe("stream with RK table") {
    it should behave like streamWrite[DataRK]

  }

}
