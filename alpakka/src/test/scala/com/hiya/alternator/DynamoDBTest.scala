package com.hiya.alternator

import akka.Done
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.syntax._
import com.hiya.alternator.util.{DataPK, DataRK, LocalDynamoDB}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class DynamoDBTest extends AnyFunSpec with Matchers with BeforeAndAfterAllConfigMap {

  case class ExampleData(pk: String, intValue: Int, stringValue: String)
  object ExampleData {
    implicit val format: CompoundDynamoFormat[ExampleData] = semiauto.deriveCompound
    implicit val schema: TableSchema.Aux[ExampleData, String] = TableSchema.schemaWithPK[String, ExampleData]("pk", _.pk)
  }


  class ExampleDB(name: String)(implicit val client: DynamoDbAsyncClient, system: ClassicActorSystemProvider)
  {
    import Table.parasitic

    val table = Table.tableWithPK[ExampleData](name)

    def get(key: String): Future[Option[ExampleData]] = table.get(key).throwErrors

    def put(data: ExampleData): Future[Done] = table.put(data)

    def delete(key: String): Future[Done] = table.delete(key)

  }

  implicit val system = ActorSystem()
  implicit val materializer = Materializer.matFromSystem
  implicit var client: DynamoDbAsyncClient = LocalDynamoDB.client(Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(8484))
  val timeout = 3.seconds

  def wait[T](body: Future[T]): T = {
    Await.result(body, timeout)
  }

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap)
    client = LocalDynamoDB.client(configMap.get("dynamoDBLocalPort").map(_.asInstanceOf[String].toInt).getOrElse(8484))
  }

  describe("DynamoDB") {
    it("can read/write items in custom test table") {

      val schema: (String, ScalarAttributeType) = "pk" -> ScalarAttributeType.S
      val tableName = s"test-table-${UUID.randomUUID()}"

      val exampleDBInstance = new ExampleDB(tableName)
      val exampleDBInstance2 = new ExampleDB(tableName)
      LocalDynamoDB.withTable(client)(tableName)(schema) {
        val key = "primaryKey"

        val data = ExampleData(key, 12, "string value")
        wait(exampleDBInstance.get(key)) shouldBe None

        wait(exampleDBInstance.put(data)) shouldBe Done

        wait(exampleDBInstance.get(key)) shouldBe Some(data)
        wait(exampleDBInstance2.get(key)) shouldBe Some(data)

        wait(exampleDBInstance.delete(key)) shouldBe Done
        wait(exampleDBInstance.get(key)) shouldBe None
        wait(exampleDBInstance2.get(key)) shouldBe None
      }
    }
  }

  private val TEST_TIMEOUT: FiniteDuration = 20.seconds
  private implicit val writer: ActorRef[BatchedWriteBehavior.BatchedRequest] =
    system.spawn(BatchedWriteBehavior(client, 10.millis, (_: Int) => 10.millis), "writer")
  private implicit val askTimeout: Timeout = 60.seconds
  private implicit val scheduler: Scheduler = system.scheduler.toTyped

  describe("query") {
    def withRangeData[T](num: Int, payload: Option[String] = None)(f: TableWithRange[DataRK, String, String] => T): T = {
      DataRK.config.withTable(client) { table =>
        Await.result({
          Source(List(num, 11))
            .flatMapConcat(i =>
              Source(0 until i).map(j =>
                DataRK(i.toString, j.toString, payload.getOrElse(s"$i/$j"))
              )
            )
            .mapAsync(100)(table.batchedPut)
            .runWith(Sink.ignore)
        }, TEST_TIMEOUT)
        f(table)
      }
    }

    it("should compile =") {
      import Table.parasitic

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk == "3").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe List(DataRK("5", "3", "5/3"))
    }

    it("should compile <") {
      import Table.parasitic

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk < "3").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe (0 until 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile <=") {
      import Table.parasitic

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk <= "3").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe (0 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >") {
      import Table.parasitic

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk > "3").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe (4 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >=") {
      import Table.parasitic

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk >= "3").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe (3 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile between") {
      import Table.parasitic

      val result = withRangeData(5) { table =>
        Await.result(table.query(pk = "5", rk.between("2", "3")).runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe (2 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile startswith") {
      import Table.parasitic

      val result = withRangeData(13) { table =>
        Await.result(table.query(pk = "13", rk.beginsWith("1")).runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result shouldBe (1 :: (10 until 13).toList).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work without rk condition") {
      import Table.parasitic

      val result = withRangeData(13) { table =>
        Await.result(table.query(pk = "13").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result should contain theSameElementsAs (0 until 13).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work with a lots of data") {
      import Table.parasitic

      val payload = "0123456789abcdefghijklmnopqrstuvwxyz" * 1000

      val result = withRangeData(1000, payload = Some(payload)) { table =>
        Await.result(table.query(pk = "1000").runWith(Sink.seq).throwErrors, TEST_TIMEOUT)
      }

      result should have size(1000)
    }
  }


  describe("scan") {
    def withData[T](f: Table[DataPK, String] => T): T = {
      DataPK.config.withTable(client) { table =>
        Await.result({
          Source(1 to 1000)
            .map(i => DataPK(i.toString, i))
            .mapAsync(100)(table.batchedPut)
            .runWith(Sink.ignore)
        }, TEST_TIMEOUT)
        f(table)
      }
    }

    it("should scan table twice") {
      withData { table =>
        Await.result(table.scan().runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
        Await.result(table.scan().runWith(Sink.seq), TEST_TIMEOUT).size shouldBe 1000
      }
    }
  }
}
