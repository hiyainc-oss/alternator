package com.hiya.alternator.cats

import cats.MonadThrow
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.hiya.alternator.aws1._
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.{DynamoDB, DynamoDBTestBase}
import fs2.Stream


class DynamoDBTest extends DynamoDBTestBase[IO, Stream[IO, *], AmazonDynamoDBAsync] {
  override protected val client: AmazonDynamoDBAsync = LocalDynamoDB.client[AmazonDynamoDBAsync]()
  override protected implicit val dbr: DynamoDB[IO, Stream[IO, *], AmazonDynamoDBAsync] = CatsAws1.forIO
  override protected implicit val monadF: MonadThrow[IO] = IO.asyncForIO
  override protected implicit val monadS: MonadThrow[Stream[IO, *]] = Stream.monadErrorInstance
  override protected def eval[T](body: IO[T]): T = body.unsafeRunSync()
  override protected def list[T](body: Stream[IO, T]): IO[List[T]] = body.compile.to(List)
}
