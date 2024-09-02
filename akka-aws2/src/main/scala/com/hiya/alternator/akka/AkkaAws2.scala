package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.stream.scaladsl.Source
import com.hiya.alternator.aws2.{Aws2Table, Aws2TableWithRangeKey}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{DynamoDB, TableLike, TableWithRangeKeyLike}
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

import scala.concurrent.Future

class AkkaAws2(implicit val system: ClassicActorSystemProvider)
  extends DynamoDB[Future, Source[*, NotUsed], DynamoDbAsyncClient] {
  import com.hiya.alternator.akka.JdkCompat._

  override def eval[T](f: => Future[T]): Source[T, NotUsed] = Source.lazyFuture(() => f)
  override def evalMap[A, B](in: Source[A, NotUsed])(f: A => Future[B]): Source[B, NotUsed] = in.mapAsync(1)(f)
  override def bracket[T, B](
    acquire: => Future[T]
  )(release: T => Future[Unit])(s: T => Source[B, NotUsed]): Source[B, NotUsed] = {
    Source.lazyFuture(() => acquire).flatMapConcat { t =>
      s(t).watchTermination() { (_, done) =>
        done.onComplete(_ => release(t))
      }
    }
  }

  override def get[V, PK](table: TableLike[DynamoDbAsyncClient, V, PK], pk: PK): Future[Option[Result[V]]] = {
    table.client.getItem(Aws2Table(table).get(pk).build()).asScala.map(Aws2Table(table).deserialize)
  }

  override def put[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]]
  ): Future[Boolean] =
    condition match {
      case Some(condition) =>
        table.client
          .putItem(Aws2Table(table).put(item, condition).build())
          .asScala
          .map(_ => true)
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        table.client
          .putItem(Aws2Table(table).put(item).build())
          .asScala
          .map(_ => true)
    }

  override def delete[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ): Future[Boolean] =
    condition match {
      case Some(condition) =>
        table.client
          .deleteItem(Aws2Table(table).delete(key, condition).build())
          .asScala
          .map(_ => true)
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        table.client
          .deleteItem(Aws2Table(table).delete(key).build())
          .asScala
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
  ): Future[Unit] =
    client
      .createTable(Aws2Table.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes).build())
      .asScala
      .map(_ => ())

  override def dropTable(client: DynamoDbAsyncClient, tableName: String): Future[Unit] =
    client.deleteTable(model.DeleteTableRequest.builder().tableName(tableName).build()).asScala.map(_ => ())

  override def scan[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    segment: Option[Segment]
  ): Source[Result[V], NotUsed] = {
    Source
      .fromPublisher(table.client.scanPaginator(Aws2Table(table).scan(segment).build()))
      .mapConcat(data => Aws2Table(table).deserialize(data))
  }

  override def query[V, PK, RK](
                                 table: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK],
                                 pk: PK,
                                 rk: RKCondition[RK]
  ): Source[Result[V], NotUsed] =
    Source
      .fromPublisher(table.client.queryPaginator(Aws2TableWithRangeKey(table).query(pk, rk).build()))
      .mapConcat(data => Aws2TableWithRangeKey(table).deserialize(data))

  override type BatchGetItemResponse = model.BatchGetItemResponse
  override type BatchWriteItemResponse = model.BatchWriteItemResponse

  override def batchGet[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    keys: Seq[PK]
  ): Future[BatchGetItemResponse] =
    table.client.batchGetItem(Aws2Table(table).batchGet(keys).build()).asScala

  override def batchWrite[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    values: Seq[Either[PK, V]]
  ): Future[BatchWriteItemResponse] =
    table.client.batchWriteItem(Aws2Table(table).batchWrite(values).build()).asScala

}

object AkkaAws2 {
  def apply()(implicit system: ClassicActorSystemProvider) = new AkkaAws2
}
