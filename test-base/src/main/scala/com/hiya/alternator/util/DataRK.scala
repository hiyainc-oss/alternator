package com.hiya.alternator.util

import com.hiya.alternator.schema.{TableSchema, TableSchemaWithRange}
import com.hiya.alternator.testkit.{LocalDynamoDB, LocalDynamoPartial}
import com.hiya.alternator.{Table, TableWithRange}
import com.hiya.alternator.DynamoDBClient

case class DataRK(key: String, range: String, value: String)

object DataRK {
  import com.hiya.alternator.generic.auto._

  type T[C <: DynamoDBClient] = TableWithRange[C, DataRK, String, String]

  private implicit val tableSchemaWithRK: TableSchemaWithRange.Aux[DataRK, String, String] =
    TableSchema.schemaWithRK[DataRK, String, String]("key", "range", x => x.key -> x.range)

  implicit val config: TableConfig[DataRK, (String, String), T] =
    new TableConfig[DataRK, (String, String), T] {

      override def withTable[F[_], S[_], C <: DynamoDBClient](
        client: C
      ): LocalDynamoPartial[TableWithRange[C, DataRK, String, String], C] =
        LocalDynamoDB.withRandomTableRK[C, DataRK](client)

      override def table[C <: DynamoDBClient](tableName: String, client: C): TableWithRange[C, DataRK, String, String] =
        Table.tableWithRK[DataRK](tableName).withClient(client)

      override def createData(i: Int, v: Option[Int]): ((String, String), DataRK) = {
        val pk = i.toString
        val rk = if (i % 2 > 0) "a" else "b"
        pk -> rk -> DataRK(pk, rk, v.getOrElse(i).toString)
      }
    }
}
