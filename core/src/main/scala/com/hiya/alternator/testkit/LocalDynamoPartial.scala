package com.hiya.alternator.testkit

import cats.Monad
import cats.syntax.all._
import com.hiya.alternator.{DynamoDB, DynamoDBItem, Table}

class LocalDynamoPartial[+R, C](client: C, tableName: String, magnet: SchemaMagnet, value: R) {
  def eval[F[_]: Monad, T](f: R => F[T])(implicit F: DynamoDBItem[F, C]): F[T] = {
    for {
      _ <- Table.create(tableName, magnet.hashKey, magnet.rangeKey, attributes = magnet.attributes, client = client)
      ret <- f(value)
      _ <- Table.drop(tableName, client)
    } yield ret
  }

  def source[F[_], S[_], T](f: R => S[T])(implicit DB: DynamoDB[F, S, C]): S[T] =
    DB.bracket(
      Table.create[F, C](tableName, magnet.hashKey, magnet.rangeKey, attributes = magnet.attributes, client = client)
    )(_ => Table.drop[F, C](tableName, client))(_ => f(value))
}
