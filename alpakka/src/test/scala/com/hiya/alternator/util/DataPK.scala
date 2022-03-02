package com.hiya.alternator.util

import com.hiya.alternator.testkit.{LocalDynamoDB, Timeout}
import com.hiya.alternator.{Table, TableSchema}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.util.UUID
import scala.concurrent.ExecutionContext

case class DataPK(key: String, value: Int)

object DataPK {
  import com.hiya.alternator.generic.auto._

  implicit val tableSchemaWithPK: TableSchema.Aux[DataPK, String] =
    TableSchema.schemaWithPK[String, DataPK]("key", _.key)


  implicit val config = new TableConfig[DataPK] {
    override type Key = String
    override type TableType = Table[DataPK, String]


    override def table(tableName: String): Table[DataPK, String] =
      Table.tableWithPK[DataPK](tableName)

    override def createData(i: Int, v: Option[Int]): (String, DataPK) = {
      i.toString -> DataPK(i.toString, v.getOrElse(i))
    }

    override def withTable[T](client: DynamoDbAsyncClient)(f: Table[DataPK, String] => T)
                             (implicit ec: ExecutionContext, timeout: Timeout): T = {
      val tableName = s"test-table-${UUID.randomUUID()}"
      LocalDynamoDB.withTable(client)(tableName)(LocalDynamoDB.schema[DataPK])(
        f(table(tableName))
      )
    }
  }
}
