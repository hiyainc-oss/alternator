package com.hiya.alternator.testkit

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator.{DynamoDB, DynamoDBItem, Table}

class LocalDynamoPartial[+R, C](client: C, tableName: String, magnet: SchemaMagnet, value: R) {
  def eval[F[_]: MonadThrow, T](f: R => F[T])(implicit DB: DynamoDBItem[F, C]): F[T] = {
    Table
      .create(tableName, magnet.hashKey, magnet.rangeKey, attributes = magnet.attributes, client = client)
      .flatMap { _ =>
        f(value).attemptTap(_ => Table.drop[F, C](tableName, client))
      }
  }

  def source[F[_], S[_], T](f: R => S[T])(implicit DB: DynamoDB[F, S, C]): S[T] =
    DB.bracket(
      Table.create[F, C](tableName, magnet.hashKey, magnet.rangeKey, attributes = magnet.attributes, client = client)
    )(_ => Table.drop[F, C](tableName, client))(_ => f(value))
}
