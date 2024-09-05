package com.hiya.alternator.cats

import _root_.cats.effect._
import com.hiya.alternator._
import com.hiya.alternator.aws2.internal.Aws2DynamoDB
import com.hiya.alternator.aws2.{Aws2Table, Aws2TableWithRangeKey}
import com.hiya.alternator.cats.internal.CatsBase
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import fs2.Stream
import fs2.interop.reactivestreams.PublisherOps
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.util.concurrent.CompletableFuture

class CatsAws2[F[+_]](protected override implicit val F: Async[F])
  extends Aws2DynamoDB[F, Stream[F, *]]
  with CatsBase[F] {
  override protected def async[T](f: => CompletableFuture[T]): F[T] = {
    Async[F].fromCompletableFuture(Async[F].delay(f))
  }

  override def scan[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]]
  ): Stream[F, Result[V]] =
    table.client
      .scanPaginator(Aws2Table(table).scan(segment, condition).build())
      .toStreamBuffered(1)
      .flatMap(data => Stream.emits(Aws2Table(table).deserialize(data)))

  override def query[V, PK, RK](
    table: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]]
  ): Stream[F, Result[V]] =
    table.client
      .queryPaginator(Aws2TableWithRangeKey(table).query(pk, rk, condition).build())
      .toStreamBuffered(1)
      .flatMap { data => fs2.Stream.emits(Aws2TableWithRangeKey(table).deserialize(data)) }
}

object CatsAws2 {
  val forIO: CatsAws2[IO] = forAsync[IO]

  def forAsync[F[+_]: Async]: CatsAws2[F] = new CatsAws2[F]

  def apply[F[+_]](implicit F: CatsAws2[F]): CatsAws2[F] = F
}
