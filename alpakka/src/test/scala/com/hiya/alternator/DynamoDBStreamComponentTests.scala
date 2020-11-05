package com.hiya.alternator

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors}
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class DynamoDBStreamComponentTests extends AnyFunSpec with Matchers with Inside with BeforeAndAfterAll with Inspectors {

  private implicit val system = ActorSystem()

  private val TEST_TIMEOUT = 20.seconds

  private implicit val dbClient = LocalDynamoDB.client(Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(8484))

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), 60.seconds)
    super.afterAll()
  }

  def withTable[T](p : => T): T = {
    LocalDynamoDB.withTable(dbClient)("test_pk")("key" -> ScalarAttributeType.S)(p)
  }

  def withRKTable[T](p : => T): T = {
    LocalDynamoDB.withTable(dbClient)("test_rk")("key" -> ScalarAttributeType.S, "range" -> ScalarAttributeType.S)(p)
  }


  case class DataPK(key: String, value: Int)
  case class DataRK(key: String, range: String, value: Int)

  import com.hiya.alternator.generic.auto._
  private implicit val tableSchemaWithPK = TableSchema.schemaWithPK[String, DataPK]("key")
  private val tableWithPK = Table.tableWithPK[DataPK]("test_pk")
  private implicit val tableSchemaWithRK = TableSchema.schemaWithRK[String, String, DataRK]("key", "range")
  private val tableWithRK = Table.tableWithPK[DataRK]("test_rk")

//  private val dbStream = new DynamoDBStream(None)(system)
//  private val writer = dbStream.write(DynamoStreamConfig(10.seconds, None, 2500, Timeout(10.seconds), 100.millis))(PartialFunction.empty)
//  private val reader = dbStream.read(DynamoStreamConfig(10.seconds, None, 10000, Timeout(10.seconds), 100.millis))(PartialFunction.empty)
  private implicit val reader: ActorRef[BatchedReadBehavior.BatchedRequest] = system.spawn(BatchedReadBehavior(dbClient, 10.millis, (_: Int) => 10.millis), "reader")

  def writeData(nums: immutable.Iterable[Int]): Future[Seq[Done]] = {
// FIXME
//    Source(nums)
//      .map(v => DataPK(v.toString, v))
//      .map(v => tableWithPK.write(v) -> Done)
//      .via(writer.flow)
//      .grouped(Int.MaxValue)
//      .runWith(Sink.head)
      Source(nums)
        .map(v => DataPK(v.toString, v))
        .mapAsync(10)(tableWithPK.put)
        .grouped(Int.MaxValue)
        .runWith(Sink.head)
  }

  def getRangeKey(i: Int): String = if (i % 2 > 0) "a" else "b"

  def writeDataRK(nums: immutable.Iterable[Int]): Future[Seq[Done]] = {
// FIXME
//    Source(nums)
//      .map(v => DataRK(v.toString, getRangeKey(v), v))
//      .map(d => tableWithRK.write(d) -> Done)
//      .via(writer.flow)
//      .grouped(Int.MaxValue)
//      .runWith(Sink.head)
    Source(nums)
      .map(v => DataRK(v.toString, getRangeKey(v), v))
      .mapAsync(10)(tableWithRK.put)
      .grouped(Int.MaxValue)
      .runWith(Sink.head)
  }


  describe("stream writer") {
    implicit val timeout: Timeout = 1.second
    implicit val scheduler: Scheduler = system.scheduler.toTyped

    it("should write data") {
      withTable {
        val r = Await.result(writeData(1 to 100), TEST_TIMEOUT)
        r.size shouldBe 100
      }
    }

    it("should read data") {

      withTable {
        Await.result(writeData(1 to 100), TEST_TIMEOUT)
        val r: Flow[(String, Int), (Option[DataPK], Int), NotUsed] = tableWithPK.batchedGetFlowUnordered[Int](10)
        val f = Source(1 to 100)
          .map(k => k.toString -> k)
          .via(r)
          .grouped(Int.MaxValue)
          .runWith(Sink.head)
        val result = Await.result(f, TEST_TIMEOUT)

        result.size shouldBe 100

        forAll (result) { case (data, pt) =>
          inside(data) {
            case Some(DataPK(_, p)) => p shouldBe pt
          }
        }
      }
    }

    it("should read empty table") {
      withTable {
        val r: Flow[(String, Int), (Option[DataPK], Int), NotUsed] = tableWithPK.batchedGetFlowUnordered[Int](10)
        val f = Source(1 to 100)
          .map(k => k.toString -> k)
          .via(r)
          .grouped(Int.MaxValue)
          .runWith(Sink.head)
        val result = Await.result(f, TEST_TIMEOUT)

        result.size shouldBe 100

        forAll (result) { case (data, _) =>
            inside(data) {
              case None =>
            }
        }
      }
    }
  }

//  describe("scan") {
//    it("should read table twice") {
//      withTable {
//        val r = Await.result(writeData(1 to 1000), TEST_TIMEOUT)
//        r.size shouldBe 1000
//
//        Await.result(tableWithPK.scan(dbStream).runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
//
//        Await.result(tableWithPK.scan(dbStream).runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
//      }
//    }
//
//    def scanAll(): immutable.Seq[DataPK] = {
//      val db = for { i <- 0 until 4 }
//        yield tableWithPK.scan(dbStream, Some(i -> 4)).runWith(Sink.seq)
//      import system.dispatcher
//      Await.result(Future.sequence(db).map(_.flatten), TEST_TIMEOUT * 2)
//    }
//
//    it("should read table twice with parallelism") {
//      withTable {
//        Await.result(writeData(1 to 1000), TEST_TIMEOUT)
//        scanAll().toSet shouldBe (1 to 1000).map { i => DataPK(i.toString, i) }.toSet
//        scanAll().toSet shouldBe (1 to 1000).map { i => DataPK(i.toString, i) }.toSet
//      }
//    }
//  }

  describe("stream writer with RK table") {
    implicit val timeout: Timeout = 1.second
    implicit val scheduler: Scheduler = system.scheduler.toTyped

    it("should write data") {
      withRKTable {
        val r = Await.result(writeDataRK(1 to 100), TEST_TIMEOUT)
        r.size shouldBe 100
      }
    }

    it("should read data") {
      withRKTable {
        Await.result(writeDataRK(1 to 100), TEST_TIMEOUT)
        val r: Flow[((String, String), Int), (Option[DataRK], Int), NotUsed] = tableWithRK.batchedGetFlowUnordered[Int](10)
        val f = Source(1 to 100)
          .map(k => (k.toString, getRangeKey(k)) -> k)
          .via(r)
          .grouped(Int.MaxValue)
          .runWith(Sink.head)
        val result = Await.result(f, TEST_TIMEOUT)

        result.size shouldBe 100

        forAll (result) { case (data, pt) =>
          inside(data) {
            case Some(DataRK(_, _, p)) => p shouldBe pt
          }
        }
      }
    }

    it("should read empty table") {
      withRKTable {
        val r: Flow[((String, String), Int), (Option[DataRK], Int), NotUsed] = tableWithRK.batchedGetFlowUnordered[Int](10)
        val f = Source(1 to 100)
          .map(k => (k.toString, getRangeKey(k)) -> k)
          .via(r)
          .grouped(Int.MaxValue)
          .runWith(Sink.head)
        val result = Await.result(f, TEST_TIMEOUT)

        result.size shouldBe 100

        forAll (result) { case (data, _) =>
            inside(data) {
              case None =>
            }
        }
      }
    }
  }

//  describe("scan with RK table") {
//    it("should read table twice") {
//      withRKTable {
//        val r = Await.result(writeDataRK(1 to 1000), TEST_TIMEOUT)
//        r.size shouldBe 1000
//
//        Await.result(tableWithRK.scan(dbStream).runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
//
//        Await.result(tableWithRK.scan(dbStream).runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
//      }
//    }
//
//    def scanAll(): immutable.Seq[DataRK] = {
//      val db = for { i <- 0 until 4 }
//        yield tableWithRK.scan(dbStream, Some(i -> 4)).runWith(Sink.seq)
//      import system.dispatcher
//      Await.result(Future.sequence(db).map(_.flatten), TEST_TIMEOUT * 2)
//    }
//
//    it("should read table twice with parallelism") {
//      withRKTable {
//        Await.result(writeDataRK(1 to 1000), TEST_TIMEOUT)
//        scanAll().toSet shouldBe (1 to 1000).map { i => DataRK(i.toString, getRangeKey(i), i) }.toSet
//        scanAll().toSet shouldBe (1 to 1000).map { i => DataRK(i.toString, getRangeKey(i), i) }.toSet
//      }
//    }
//  }
}
