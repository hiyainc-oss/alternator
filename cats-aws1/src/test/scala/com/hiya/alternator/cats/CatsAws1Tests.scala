package com.hiya.alternator.cats

import cats.MonadThrow
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.hiya.alternator.aws1._
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.{DynamoDB, DynamoDBTestBase}
import fs2.Stream
import com.hiya.alternator.aws1.internal.Aws1DynamoDBClient
import com.hiya.alternator.DynamoDBClient


class CatsAws1Tests extends DynamoDBTestBase[IO, Stream[IO, *], Aws1DynamoDBClient] {
  override protected val client: Aws1DynamoDBClient = LocalDynamoDB.client[Aws1DynamoDBClient]()
  override protected implicit val DB: DynamoDB.Aux[IO, Stream[IO, *], Aws1DynamoDBClient] = CatsAws1.forIO
  override protected implicit val monadF: MonadThrow[IO] = IO.asyncForIO
  override protected implicit val monadS: MonadThrow[Stream[IO, *]] = Stream.monadErrorInstance
  override protected def eval[T](body: IO[T]): T = body.unsafeRunSync()
  override protected def list[T](body: Stream[IO, T]): IO[List[T]] = body.compile.to(List)
  override implicit protected def hasOverride: DynamoDBClient.HasOverride[Aws1DynamoDBClient, _] = DynamoDBClient.HasOverride.forNothing
}
