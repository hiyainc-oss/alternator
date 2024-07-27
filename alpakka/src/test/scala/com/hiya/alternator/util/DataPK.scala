package com.hiya.alternator.util

import akka.actor.ClassicActorSystemProvider
import com.hiya.alternator.alpakka.{Alpakka, AlpakkaTableOps}
import com.hiya.alternator.testkit.{LocalDynamoDB, Timeout}
import com.hiya.alternator.{Table, TableSchema}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.util.UUID
import scala.concurrent.ExecutionContext

case class DataPK(key: String, value: Int)

object DataPK {
  import com.hiya.alternator.generic.auto._

  implicit val tableSchemaWithPK: TableSchema.Aux[DataPK, String] =
    TableSchema.schemaWithPK[DataPK, String]("key", _.key)


  implicit val config: TableConfig.Aux[DataPK, String, AlpakkaTableOps[DataPK, String]] = new TableConfig[DataPK] {
    override type Key = String
    override type TableType = AlpakkaTableOps[DataPK, String]


    override def table(tableName: String, client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider): AlpakkaTableOps[DataPK, String] =
      Table.tableWithPK[DataPK](tableName).withClient(Alpakka(client))


    override def createData(i: Int, v: Option[Int]): (String, DataPK) = {
      i.toString -> DataPK(i.toString, v.getOrElse(i))
    }

    override def withTable[T](client: DynamoDbAsyncClient)(f: AlpakkaTableOps[DataPK, String] => T)
                             (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T = {
      val tableName = s"test-table-${UUID.randomUUID()}"
      LocalDynamoDB.withTable(client)(tableName)(LocalDynamoDB.schema[DataPK])(
        f(table(tableName, client))
      )

    }
  }
}
