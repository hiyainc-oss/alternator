package com.hiya.alternator.testkit

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator.{DynamoDB, DynamoDBClient}

final case class LocalDynamoPartial[+R, C <: DynamoDBClient](
  client: C,
  tableName: String,
  magnet: SchemaMagnet,
  value: R
) {
  def eval[F[_]: MonadThrow, T](f: R => F[T])(implicit DB: DynamoDB.Client[F, C]): F[T] = {
    DB.createTable(
      client,
      tableName,
      magnet.hashKey,
      magnet.rangeKey,
      attributes = magnet.attributes,
      globalSecondaryIndexes = magnet.globalSecondaryIndexes
    ).flatMap { _ =>
      f(value).attemptTap(_ => DB.dropTable(client, tableName))
    }
  }

  def source[F[_], S[_], T](f: R => S[T])(implicit DB: DynamoDB.Aux[F, S, C]): S[T] =
    DB.bracket[Unit, T](
      DB.createTable(
        client,
        tableName,
        magnet.hashKey,
        magnet.rangeKey,
        attributes = magnet.attributes,
        globalSecondaryIndexes = magnet.globalSecondaryIndexes
      )
    )(_ => DB.dropTable(client, tableName))(_ => f(value))
}
