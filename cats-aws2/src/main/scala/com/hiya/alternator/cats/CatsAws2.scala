package com.hiya.alternator.cats

import cats.effect.{Async, IO}
import cats.syntax.all._
import com.hiya.alternator.aws2.{Aws2Table, Aws2TableWithRangeKey}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{DynamoDB, TableLike, TableWithRangeKeyLike}
import fs2.Stream
import fs2.interop.reactivestreams.PublisherOps
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

class CatsAws2[F[_]: Async] extends DynamoDB[F, Stream[F, *], DynamoDbAsyncClient] {
  override type BatchGetItemResponse = model.BatchGetItemResponse
  override type BatchWriteItemResponse = model.BatchWriteItemResponse

  override def get[V, PK](table: TableLike[DynamoDbAsyncClient, V, PK], pk: PK): F[Option[Result[V]]] =
    Async[F]
      .fromCompletableFuture(Async[F].delay {
        table.client.getItem(Aws2Table(table).get(pk).build())
      })
      .map(Aws2Table(table).deserialize)

  override def put[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        Async[F]
          .fromCompletableFuture(Async[F].delay { table.client.putItem(Aws2Table(table).put(item, condition).build()) })
          .map(_ => true)
          .recover { case _: model.ConditionalCheckFailedException => false }

      case None =>
        Async[F]
          .fromCompletableFuture(Async[F].delay { table.client.putItem(Aws2Table(table).put(item).build()) })
          .map(_ => true)
    }

  override def delete[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        Async[F]
          .fromCompletableFuture(Async[F].delay {
            table.client.deleteItem(Aws2Table(table).delete(key, condition).build())
          })
          .map(_ => true)
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        Async[F]
          .fromCompletableFuture(Async[F].delay {
            table.client.deleteItem(Aws2Table(table).delete(key).build())
          })
          .map(_ => true)
    }

  override def createTable(
    client: DynamoDbAsyncClient,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)]
  ): F[Unit] =
    Async[F]
      .fromCompletableFuture(Async[F].delay {
        client.createTable(
          Aws2Table.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes).build()
        )
      })
      .map(_ => ())

  override def dropTable(client: DynamoDbAsyncClient, tableName: String): F[Unit] =
    Async[F]
      .fromCompletableFuture(Async[F].delay {
        client.deleteTable(Aws2Table.dropTable(tableName).build())
      })
      .map(_ => ())

  override def scan[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    segment: Option[Segment]
  ): Stream[F, Result[V]] =
    table.client
      .scanPaginator(Aws2Table(table).scan(segment).build())
      .toStreamBuffered(1)
      .flatMap(data => Stream.emits(Aws2Table(table).deserialize(data)))

  override def query[V, PK, RK](
                                 table: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK],
                                 pk: PK,
                                 rk: RKCondition[RK]
  ): Stream[F, Result[V]] =
    table.client
      .queryPaginator(Aws2TableWithRangeKey(table).query(pk, rk).build())
      .toStreamBuffered(1)
      .flatMap { data => fs2.Stream.emits(Aws2TableWithRangeKey(table).deserialize(data)) }

  override def eval[T](f: => F[T]): Stream[F, T] = Stream.eval(f)

  override def bracket[T, B](acquire: => F[T])(release: T => F[Unit])(s: T => Stream[F, B]): Stream[F, B] =
    Stream.bracket(acquire)(release).flatMap(s)

  override def evalMap[A, B](in: Stream[F, A])(f: A => F[B]): Stream[F, B] =
    in.evalMap(f)

  override def batchGet[V, PK](table: TableLike[DynamoDbAsyncClient, V, PK], keys: Seq[PK]): F[BatchGetItemResponse] =
    Async[F]
      .fromCompletableFuture(Async[F].delay {
        table.client.batchGetItem(Aws2Table(table).batchGet(keys).build())
      })

  override def batchWrite[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    values: Seq[Either[PK, V]]
  ): F[BatchWriteItemResponse] =
    Async[F]
      .fromCompletableFuture(Async[F].delay {
        table.client.batchWriteItem(Aws2Table(table).batchWrite(values).build())
      })
}

object CatsAws2 {
  val forIO: CatsAws2[IO] = forAsync[IO]

  def forAsync[F[_]: Async]: CatsAws2[F] = new CatsAws2[F]

  def apply[F[_]](implicit F: CatsAws2[F]): CatsAws2[F] = F
}
