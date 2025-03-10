package com.hiya.alternator.cats

import cats.MonadThrow
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.hiya.alternator.{DynamoDB, DynamoDBTestBase}
import com.hiya.alternator.aws2._
import com.hiya.alternator.testkit.LocalDynamoDB
import fs2.Stream
import com.hiya.alternator.aws2.Aws2DynamoDBClient

class CatsAws2Tests extends DynamoDBTestBase[IO, Stream[IO, *], Aws2DynamoDBClient] {
  override protected val client: Aws2DynamoDBClient = LocalDynamoDB.client[Aws2DynamoDBClient]()
  override protected implicit val DB: DynamoDB.Aux[IO, Stream[IO, *], Aws2DynamoDBClient] = CatsAws2.forIO
  override protected implicit val monadF: MonadThrow[IO] = IO.asyncForIO
  override protected implicit val monadS: MonadThrow[Stream[IO, *]] = Stream.monadErrorInstance
  override protected def eval[T](body: IO[T]): T = body.unsafeRunSync()
  override protected def list[T](body: Stream[IO, T]): IO[List[T]] = body.compile.to(List)
}
