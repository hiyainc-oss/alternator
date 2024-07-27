package com.hiya.alternator.cats.util

import akka.actor.ClassicActorSystemProvider
import cats.effect.IO
import com.hiya.alternator.cats.{Cats, CatsTableOps}
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


  implicit val config: TableConfig[DataPK, String, CatsTableOps[IO, DataPK, String]] =
    new TableConfig[DataPK, String, CatsTableOps[IO, DataPK, String]] {

      override def table(tableName: String, client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider): CatsTableOps[IO, DataPK, String] =
        Table.tableWithPK[DataPK](tableName).withClient(Cats[IO](client))


      override def createData(i: Int, v: Option[Int]): (String, DataPK) = {
        i.toString -> DataPK(i.toString, v.getOrElse(i))
      }

      override def withTable[T](client: DynamoDbAsyncClient)(f: CatsTableOps[IO, DataPK, String] => T)
                               (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T = {
        val tableName = s"test-table-${UUID.randomUUID()}"
        LocalDynamoDB.withTable(client)(tableName)(LocalDynamoDB.schema[DataPK])(
          f(table(tableName, client))
        )

      }
    }
}
