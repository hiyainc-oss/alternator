package com.hiya.alternator.util

import akka.actor.ClassicActorSystemProvider
import com.hiya.alternator.alpakka.{Alpakka, AlpakkaTableOpsWithRange}
import com.hiya.alternator.testkit.{LocalDynamoDB, Timeout}
import com.hiya.alternator.{Table, TableSchema, TableSchemaWithRange}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.util.UUID
import scala.concurrent.ExecutionContext

case class DataRK(key: String, range: String, value: String)

object DataRK {
  import com.hiya.alternator.generic.auto._

  private implicit val tableSchemaWithRK: TableSchemaWithRange.Aux[DataRK, String, String] =
    TableSchema.schemaWithRK[String, String, DataRK]("key", "range", x => x.key -> x.range)

  implicit val config = new TableConfig[DataRK] {
    override type Key = (String, String)
    override type TableType = AlpakkaTableOpsWithRange[DataRK, String, String]


    override def table(tableName: String, client: DynamoDbAsyncClient)
                      (implicit system: ClassicActorSystemProvider): AlpakkaTableOpsWithRange[DataRK, String, String] =
      Table.tableWithRK[DataRK](tableName).withClient(Alpakka(client))

    override def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T)
                             (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T = {
      val tableName = s"test-table-${UUID.randomUUID()}"
      LocalDynamoDB.withTable(client)(tableName)(LocalDynamoDB.schema[DataRK]) {
        f(table(tableName, client))
      }
    }

    override def createData(i: Int, v: Option[Int]): ((String, String), DataRK) = {
      val pk = i.toString
      val rk = if (i % 2 > 0) "a" else "b"
      pk -> rk -> DataRK(pk, rk, v.getOrElse(i).toString)
    }
  }
}
