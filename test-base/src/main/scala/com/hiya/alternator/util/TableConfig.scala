package com.hiya.alternator.util

import cats.Monad
import com.hiya.alternator.testkit.LocalDynamoPartial
import com.hiya.alternator.{DynamoDB, DynamoDBClient, Table}

abstract class TableConfig[Data, Key, +TableType[_ <: DynamoDBClient] <: Table[_, Data, Key]] {
  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[F[_], S[_], C <: DynamoDBClient](client: C): LocalDynamoPartial[TableType[C], C]
  def table[C <: DynamoDBClient](name: String, client: C): TableType[C]
}

object TableConfig {
  trait Partial[F[_], S[_], C <: DynamoDBClient, +TableType[_ <: DynamoDBClient] <: Table[_, _, _]] {
    def source[T](f: TableType[C] => S[T])(implicit dynamoDB: DynamoDB.Aux[F, S, C]): S[T]
    def eval[T](f: TableType[C] => F[T])(implicit dynamoDB: DynamoDB.Aux[F, S, C], F: Monad[F]): F[T]
  }
}
