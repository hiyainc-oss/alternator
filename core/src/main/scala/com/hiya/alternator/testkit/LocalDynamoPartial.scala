package com.hiya.alternator.testkit

import cats.Monad
import cats.syntax.all._
import com.hiya.alternator.{DynamoDB, DynamoDBValue, Table}

class LocalDynamoPartial[C](client: C, tableName: String, magnet: SchemaMagnet) {
  def eval[F[_]: Monad, T](f: String => F[T])(implicit F: DynamoDBValue[F, C]): F[T] = {
    for {
      _ <- Table.create(tableName, magnet.hashKey, magnet.rangeKey, attributes = magnet.attributes, client = client)
      ret <- f(tableName)
      _ <- Table.drop(tableName, client)
    } yield ret
  }

  def source[F[_], S[_], T](f: String => S[T])(implicit DB: DynamoDB[F, S, C]): S[T] =
    DB.bracket(
      Table.create[F, C](tableName, magnet.hashKey, magnet.rangeKey, attributes = magnet.attributes, client = client)
    )(_ => Table.drop[F, C](tableName, client))(_ => f(tableName))
}
