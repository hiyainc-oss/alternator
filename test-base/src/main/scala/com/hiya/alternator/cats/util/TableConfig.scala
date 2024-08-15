package com.hiya.alternator.cats.util

import cats.Monad
import com.hiya.alternator.DynamoDB

abstract class TableConfig[Data, Key, TableType[_]] {
  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[F[_], S[_], C](client: C): TableConfig.Partial[F, S, C, TableType]
  def table[C](name: String, client: C): TableType[C]
}

object TableConfig {
  trait Partial[F[_], S[_], C, TableType[_]]  {
    def source[T](f: TableType[C] => S[T])(implicit dynamoDB: DynamoDB[F, S, C]): S[T]
    def eval[T](f: TableType[C] => F[T])(implicit dynamoDB: DynamoDB[F, S, C], F: Monad[F]): F[T]
  }
}
