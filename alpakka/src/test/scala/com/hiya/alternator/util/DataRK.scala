package com.hiya.alternator.util

import com.hiya.alternator.{Table, TableSchema}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType

import java.util.UUID

case class DataRK(key: String, range: String, value: Int)

object DataRK {
  import com.hiya.alternator.generic.auto._

  private implicit val tableSchemaWithRK: TableSchema.Aux[DataRK, (String, String)] =
    TableSchema.schemaWithRK[String, String, DataRK]("key", "range", x => x.key -> x.range)

  implicit val config                                                               = new TableConfig[DataRK] {
    override type Key = (String, String)

    override def withTable[T](client: DynamoDbAsyncClient)(f: Table[DataRK, (String, String)] => T): T = {
      val tableName = s"test-table-${UUID.randomUUID()}"
      LocalDynamoDB.withTable(client)(tableName)("key" -> ScalarAttributeType.S, "range" -> ScalarAttributeType.S) {
        f(Table.tableWithPK[DataRK](tableName))
      }
    }

    override def createData(i: Int, v: Option[Int]): ((String, String), DataRK) = {
      val pk = i.toString
      val rk = if (i % 2 > 0) "a" else "b"
      pk -> rk -> DataRK(pk, rk, v.getOrElse(i))
    }
  }
}
