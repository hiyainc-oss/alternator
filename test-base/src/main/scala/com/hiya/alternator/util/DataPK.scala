package com.hiya.alternator.util

import com.hiya.alternator.schema.TableSchema
import com.hiya.alternator.testkit.{LocalDynamoDB, LocalDynamoPartial}
import com.hiya.alternator.{Table, TableLike}

case class DataPK(key: String, value: Int)

object DataPK {
  import com.hiya.alternator.generic.auto._

  implicit val tableSchemaWithPK: TableSchema.Aux[DataPK, String] =
    TableSchema.schemaWithPK[DataPK, String]("key", _.key)

  implicit val config: TableConfig[DataPK, String, TableLike[*, DataPK, String]] =
    new TableConfig[DataPK, String, TableLike[*, DataPK, String]] {
      override def table[C](name: String, client: C): TableLike[C, DataPK, String] =
        Table.tableWithPK[DataPK](name).withClient(client)

      override def createData(i: Int, v: Option[Int]): (String, DataPK) = {
        i.toString -> DataPK(i.toString, v.getOrElse(i))
      }

      override def withTable[F[_], S[_], C](client: C): LocalDynamoPartial[TableLike[C, DataPK, String], C] =
        LocalDynamoDB.withRandomTable[C, DataPK](client)
    }
}
