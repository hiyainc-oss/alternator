package com.hiya.alternator

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.hiya.alternator.generic.semiauto
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
    implicit val schema: TableSchema.Aux[ExampleData, String] = TableSchema.schemaWithPK[String, ExampleData]("pk")
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
}
