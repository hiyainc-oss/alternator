package com.hiya.alternator.util

import com.hiya.alternator.Table
import com.hiya.alternator.schema.TableSchema
import com.hiya.alternator.testkit.{LocalDynamoDB, LocalDynamoPartial}
import com.hiya.alternator.DynamoDBClient

case class DataPK(key: String, value: Int)

object DataPK {
  import com.hiya.alternator.generic.auto._

  type T[C <: DynamoDBClient] = Table[C, DataPK, String]

  implicit val tableSchemaWithPK: TableSchema.Aux[DataPK, String] =
    TableSchema.schemaWithPK[DataPK, String]("key", _.key)

  implicit val config: TableConfig[DataPK, String, T] =
    new TableConfig[DataPK, String, T] {
      override def table[C <: DynamoDBClient](name: String, client: C): Table[C, DataPK, String] =
        Table.tableWithPK[DataPK](name).withClient(client)

      override def createData(i: Int, v: Option[Int]): (String, DataPK) = {
        i.toString -> DataPK(i.toString, v.getOrElse(i))
      }

      override def withTable[F[_], S[_], C <: DynamoDBClient](client: C): LocalDynamoPartial[Table[C, DataPK, String], C] =
        LocalDynamoDB.withRandomTable[C, DataPK](client)
    }
}
