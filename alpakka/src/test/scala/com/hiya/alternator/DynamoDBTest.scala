package com.hiya.alternator

import java.util.UUID
import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.util.LocalDynamoDB
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class DynamoDBTest extends AnyFunSpec with Matchers with BeforeAndAfterAllConfigMap {

  case class ExampleData(pk: String, intValue: Int, stringValue: String)
  object ExampleData {
    implicit val format: CompoundDynamoFormat[ExampleData] = semiauto.deriveCompound
    implicit val schema: TableSchema.Aux[ExampleData, String] = TableSchema.schemaWithPK[String, ExampleData]("pk", _.pk)
  }


  class ExampleDB(name: String)(implicit val client: DynamoDbAsyncClient, mat: Materializer)
  {
    val table = Table.tableWithPK[ExampleData](name)

    def get(key: String): Future[Option[ExampleData]] = table.get(key)

    def put(data: ExampleData): Future[Done] = table.put(data)

    def delete(key: String): Future[Done] = table.delete(key)

  }

  implicit val system = ActorSystem()
  implicit val materializer = Materializer.matFromSystem
  implicit var client: DynamoDbAsyncClient = LocalDynamoDB.client()
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
