package com.hiya.alternator.util

import cats.Monad
import com.hiya.alternator.schema.{TableSchema, TableSchemaWithRange}
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.{DynamoDB, Table, TableWithRangeLike}

case class DataRK(key: String, range: String, value: String)

object DataRK {
  import com.hiya.alternator.generic.auto._

  private implicit val tableSchemaWithRK: TableSchemaWithRange.Aux[DataRK, String, String] =
    TableSchema.schemaWithRK[DataRK, String, String]("key", "range", x => x.key -> x.range)

  implicit val config: TableConfig[DataRK, (String, String), TableWithRangeLike[*, DataRK, String, String]] =
    new TableConfig[DataRK, (String, String), TableWithRangeLike[*, DataRK, String, String]] {
      override def withTable[F[_], S[_], C](
        client: C
      ): TableConfig.Partial[F, S, C, TableWithRangeLike[*, DataRK, String, String]] =
        new TableConfig.Partial[F, S, C, TableWithRangeLike[*, DataRK, String, String]] {
          override def source[T](f: TableWithRangeLike[C, DataRK, String, String] => S[T])(implicit
            dynamoDB: DynamoDB[F, S, C]
          ): S[T] = {
            LocalDynamoDB.withRandomTable(client)(LocalDynamoDB.schema[DataRK]).source { tableName =>
              f(table(tableName, client))
            }
          }

          override def eval[T](f: TableWithRangeLike[C, DataRK, String, String] => F[T])(implicit
            dynamoDB: DynamoDB[F, S, C],
            F: Monad[F]
          ): F[T] = {
            LocalDynamoDB.withRandomTable(client)(LocalDynamoDB.schema[DataRK]).eval { tableName =>
              f(table(tableName, client))
            }
          }
        }

      override def table[C](tableName: String, client: C): TableWithRangeLike[C, DataRK, String, String] =
        Table.tableWithRK[DataRK](tableName).withClient(client)

      override def createData(i: Int, v: Option[Int]): ((String, String), DataRK) = {
        val pk = i.toString
        val rk = if (i % 2 > 0) "a" else "b"
        pk -> rk -> DataRK(pk, rk, v.getOrElse(i).toString)
      }
    }
}
