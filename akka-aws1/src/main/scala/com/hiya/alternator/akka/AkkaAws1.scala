package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.stream.scaladsl.Source
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, model}
import com.hiya.alternator.aws1.{Aws1Table, Aws1TableWithRangeKey}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{DynamoDB, TableLike, TableWithRangeLike}

import java.util.concurrent.{Future => JFuture}
import scala.concurrent.{Future, Promise}



class AkkaAws1(implicit val system: ClassicActorSystemProvider) extends DynamoDB[Future, Source[*, NotUsed], AmazonDynamoDBAsync] {
  import com.hiya.alternator.akka.JdkCompat._
  import AkkaAws1.async

  override type BatchGetItemResponse = model.BatchGetItemResult
  override type BatchWriteItemResponse = model.BatchWriteItemResult

  override def eval[T](f: => Future[T]): Source[T, NotUsed] = Source.lazyFuture(() => f)
  override def evalMap[A, B](in: Source[A, NotUsed])(f: A => Future[B]): Source[B, NotUsed] = in.mapAsync(1)(f)
  override def bracket[T, B](acquire: => Future[T])(release: T => Future[Unit])(s: T => Source[B, NotUsed]): Source[B, NotUsed] = {
    Source.lazyFuture(() => acquire).flatMapConcat { t =>
      s(t).watchTermination() { (_, done) =>
        done.onComplete(_ => release(t))
      }
    }
  }

  override def get[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], pk: PK): Future[Option[Result[V]]] =
    async(table.client.getItemAsync(Aws1Table(table).get(pk), _: AsyncHandler[GetItemRequest, GetItemResult]))
      .map(Aws1Table(table).deserialize)

  override def put[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], item: V, condition: Option[ConditionExpression[Boolean]]): Future[Boolean] =
    condition match {
      case Some(condition) =>
        async(table.client.putItemAsync(Aws1Table(table).put(item, condition), _: AsyncHandler[PutItemRequest, PutItemResult]))
          .map(_ => true)
          .recover { case _: ConditionalCheckFailedException => false }

      case None =>
        async(table.client.putItemAsync(Aws1Table(table).put(item), _: AsyncHandler[PutItemRequest, PutItemResult]))
          .map(_ => true)
    }

  override def delete[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], key: PK, condition: Option[ConditionExpression[Boolean]]): Future[Boolean] =
    condition match {
      case Some(condition) =>
        async(table.client.deleteItemAsync(Aws1Table(table).delete(key, condition), _: AsyncHandler[DeleteItemRequest, DeleteItemResult]))
          .map(_ => true)
          .recover { case _: ConditionalCheckFailedException => false }

      case None =>
        async(table.client.deleteItemAsync(Aws1Table(table).delete(key), _: AsyncHandler[DeleteItemRequest, DeleteItemResult]))
          .map(_ => true)
    }

  override def createTable(client: AmazonDynamoDBAsync, tableName: String, hashKey: String, rangeKey: Option[String], readCapacity: Long, writeCapacity: Long, attributes: List[(String, ScalarType)]): Future[Unit] =
    async(
      client.createTableAsync(
        Aws1Table.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes),
        _: AsyncHandler[CreateTableRequest, CreateTableResult]
      )
    ).map(_ => ())

  override def dropTable(client: AmazonDynamoDBAsync, tableName: String): Future[Unit] = {
    async(
      client.deleteTableAsync(
        Aws1Table.dropTable(tableName),
        _: AsyncHandler[DeleteTableRequest, DeleteTableResult]
      )
    ).map(_ => ())
  }


  private def scanPaginator(f: (ScanRequest, AsyncHandler[ScanRequest, ScanResult]) => JFuture[ScanResult], request: ScanRequest): Source[ScanResult, NotUsed] = {
    Source.unfoldAsync[Option[ScanRequest], ScanResult](Some(request)) {
      case None => Future.successful(None)
      case Some(req) =>
        async(f(req, _))
          .map { result =>
            Some(Option(result.getLastEvaluatedKey).map { key =>
              req.withExclusiveStartKey(key)
            } -> result)
          }
    }
  }

  override def scan[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], segment: Option[Segment]): Source[Result[V], NotUsed] =
    scanPaginator(table.client.scanAsync, Aws1Table(table).scan(segment))
      .mapConcat(data => Aws1Table(table).deserialize(data))


  private def queryPaginator(f: (QueryRequest, AsyncHandler[QueryRequest, QueryResult]) => JFuture[QueryResult], request: QueryRequest): Source[QueryResult, NotUsed] = {
    Source.unfoldAsync[Option[QueryRequest], QueryResult](Some(request)) {
      case None => Future.successful(None)
      case Some(req) =>
        async(f(req, _))
          .map { result =>
            Some(Option(result.getLastEvaluatedKey).map { key =>
              req.withExclusiveStartKey(key)
            } -> result)
          }
    }
  }

  override def query[V, PK, RK](table: TableWithRangeLike[AmazonDynamoDBAsync, V, PK, RK], pk: PK, rk: RKCondition[RK]): Source[Result[V], NotUsed] =
    queryPaginator(table.client.queryAsync, Aws1TableWithRangeKey(table).query(pk, rk))
      .mapConcat { data => Aws1TableWithRangeKey(table).deserialize(data) }

  override def batchGet[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], keys: Seq[PK]): Future[BatchGetItemResult] =
    async(table.client.batchGetItemAsync(Aws1Table(table).batchGet(keys), _: AsyncHandler[model.BatchGetItemRequest, model.BatchGetItemResult]))

  override def batchWrite[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], values: Seq[Either[PK, V]]): Future[BatchWriteItemResult] =
    async(table.client.batchWriteItemAsync(Aws1Table(table).batchWrite(values), _: AsyncHandler[model.BatchWriteItemRequest, model.BatchWriteItemResult]))

}

object AkkaAws1 {
  def apply()(implicit system: ClassicActorSystemProvider) = new AkkaAws1

  private[akka] def async[Req <: AmazonWebServiceRequest, Resp](f: AsyncHandler[Req, Resp] => JFuture[Resp]): Future[Resp] = {
    val p = Promise[Resp]()

    val _ = f(new AsyncHandler[Req, Resp] {
      override def onError(exception: Exception): Unit = p.failure(exception)
      override def onSuccess(request: Req, result: Resp): Unit = p.success(result)
    })

    p.future
  }
}
