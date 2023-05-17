package com.hiya.alternator.cats

import akka.actor.ActorSystem
import fs2._
import cats.effect.IO
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.testkit.{LocalDynamoDB, Timeout => TestTimeout}
import com.hiya.alternator.{CompoundDynamoFormat, Table, TableSchema}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.util.UUID
import scala.concurrent.duration._
import cats.effect.unsafe.implicits.global
import com.hiya.alternator.cats.util.{DataPK, DataRK}
import com.hiya.alternator.syntax._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.{global => globalEc}


class DynamoDBTest extends AnyFunSpec with Matchers {

  case class ExampleData(pk: String, intValue: Int, stringValue: String)
  object ExampleData {
    implicit val format: CompoundDynamoFormat[ExampleData] = semiauto.deriveCompound
    implicit val schema: TableSchema.Aux[ExampleData, String] = TableSchema.schemaWithPK[ExampleData, String]("pk", _.pk)
  }


  class ExampleDB(name: String)(implicit val client: DynamoDbAsyncClient)
  {

    val table = Table.tableWithPK[ExampleData](name).withClient(Cats[IO](client))

    def get(key: String): IO[Option[ExampleData]] = table.get(key).raiseError
    def put(data: ExampleData): IO[Unit] = table.put(data)
    def delete(key: String): IO[Unit] = table.delete(key)

  }

  private implicit val client: DynamoDbAsyncClient = LocalDynamoDB.client()
  private implicit val testTimeout: TestTimeout = 60.seconds

  def wait[T](body: IO[T]): T = {
    body.unsafeRunSync()
  }

  describe("DynamoDB") {
    it("can read/write items in custom test table") {

      val tableName = s"test-table-${UUID.randomUUID()}"

      val exampleDBInstance = new ExampleDB(tableName)
      val exampleDBInstance2 = new ExampleDB(tableName)
      LocalDynamoDB.withTable(client)(tableName)(LocalDynamoDB.schema[ExampleData]) {
        val key = "primaryKey"

        val data = ExampleData(key, 12, "string value")
        wait(exampleDBInstance.get(key)) shouldBe None

        wait(exampleDBInstance.put(data)) shouldBe (())

        wait(exampleDBInstance.get(key)) shouldBe Some(data)
        wait(exampleDBInstance2.get(key)) shouldBe Some(data)

        wait(exampleDBInstance.delete(key)) shouldBe (())
        wait(exampleDBInstance.get(key)) shouldBe None
        wait(exampleDBInstance2.get(key)) shouldBe None
      }
    }
  }

  private implicit val system = ActorSystem()
  private val TEST_TIMEOUT: FiniteDuration = 20.seconds
//  private implicit val writer: ActorRef[BatchedWriteBehavior.BatchedRequest] =
//    system.spawn(alpakka.BatchedWriteBehavior(client, 10.millis), "writer")
//  private implicit val askTimeout: Timeout = 60.seconds
//  private implicit val scheduler: Scheduler = system.scheduler.toTyped
//
  describe("query") {
    def withRangeData[T](num: Int, payload: Option[String] = None)(f: CatsTableOpsWithRange[IO, DataRK, String, String] => T): T = {
      DataRK.config.withTable(client) { table =>
        Await.result({
          Stream.emits(List(num, 11))
            .flatMap(i =>
              Stream.emits(0 until i).map(j =>
                DataRK(i.toString, j.toString, payload.getOrElse(s"$i/$j"))
              )
            )
            .covary[IO]
            .mapAsync(100)(table.put)
            .compile
            .drain
            .unsafeToFuture()
        }, TEST_TIMEOUT)
        f(table)
      }
    }

    it("should compile =") {

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk == "3").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe List(DataRK("5", "3", "5/3"))
    }

    it("should compile <") {

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk < "3").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe (0 until 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile <=") {

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk <= "3").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe (0 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >") {

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk > "3").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe (4 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >=") {

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk >= "3").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe (3 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile between") {

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk.between("2", "3")).compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe (2 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile startswith") {

      val result = withRangeData(13) { table =>
        Await.result(table.query(pk = "13", rk.beginsWith("1")).compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result shouldBe (1 :: (10 until 13).toList).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work without rk condition") {

      val result = withRangeData(13) { table =>
        Await.result(table.query(pk = "13").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result should contain theSameElementsAs (0 until 13).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work with a lots of data") {

      val payload = "0123456789abcdefghijklmnopqrstuvwxyz" * 1000

      val result = withRangeData(1000, payload = Some(payload)) { table =>
        Await.result(table.query(pk = "1000").compile.to(List).unsafeToFuture().raiseError, TEST_TIMEOUT)
      }

      result should have size(1000)
    }
  }

  describe("putWhen") {
    it("should work for insert-if-not-exists") {
      DataPK.config.withTable(client) { table =>
        import com.hiya.alternator.syntax.cond._
        Await.result(table.putWhen(DataPK("new", 1000), attr("key").notExists).unsafeToFuture(), TEST_TIMEOUT) shouldBe true
        Await.result(table.putWhen(DataPK("new", 1000), attr("key").notExists).unsafeToFuture(), TEST_TIMEOUT) shouldBe false
      }
    }

    it("should work for optimistic locking") {
      DataPK.config.withTable(client) { table =>
        import com.hiya.alternator.syntax.cond._
        Await.result(table.putWhen(DataPK("new", 1000), attr("key").notExists).unsafeToFuture(), TEST_TIMEOUT) shouldBe true
        Await.result(table.putWhen(DataPK("new", 1001), attr("value") === lit(1000)).unsafeToFuture(), TEST_TIMEOUT) shouldBe true
        Await.result(table.putWhen(DataPK("new", 1001), attr("value") === lit(1000)).unsafeToFuture(), TEST_TIMEOUT) shouldBe false
      }
    }
  }


//  describe("scan") {
//    def withData[T](f: AlpakkaTableOps[DataPK, String] => T): T = {
//      DataPK.config.withTable(client) { table =>
//        Await.result({
//          Source(1 to 1000)
//            .map(i => DataPK(i.toString, i))
//            .mapAsync(100)(table.batchedPut)
//            .runWith(Sink.ignore)
//        }, TEST_TIMEOUT)
//        f(table)
//      }
//    }
//
//    it("should scan table twice") {
//      withData { table =>
//        Await.result(table.scan().runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
//        Await.result(table.scan().runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
//      }
//    }
//  }
}
