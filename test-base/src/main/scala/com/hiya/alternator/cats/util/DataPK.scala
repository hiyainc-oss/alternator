package com.hiya.alternator.cats.util

import cats.Monad
import com.hiya.alternator.schema.TableSchema
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.{DynamoDB, Table, TableLike}

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

      override def withTable[F[_], S[_], C](client: C): TableConfig.Partial[F, S, C, TableLike[*, DataPK, String]] =
        new TableConfig.Partial[F, S, C, TableLike[*, DataPK, String]] {
          override def source[T](f: TableLike[C, DataPK, String] => S[T])(implicit dynamoDB: DynamoDB[F, S, C]): S[T] = {
            LocalDynamoDB.withRandomTable(client)(LocalDynamoDB.schema[DataPK]).source { tableName =>
              f(table(tableName, client))
            }
          }

          override def eval[T](f: TableLike[C, DataPK, String] => F[T])(implicit dynamoDB: DynamoDB[F, S, C], F: Monad[F]): F[T] = {
            LocalDynamoDB.withRandomTable(client)(LocalDynamoDB.schema[DataPK]).eval { tableName =>
              f(table(tableName, client))
            }
          }
        }
    }
}
