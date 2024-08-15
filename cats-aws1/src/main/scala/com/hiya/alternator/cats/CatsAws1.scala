package com.hiya.alternator.cats

import cats.effect.{Async, IO}
import cats.syntax.all._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, model}
import com.amazonaws.services.dynamodbv2.model.{BatchGetItemResult, BatchWriteItemResult, ConditionalCheckFailedException, CreateTableRequest, CreateTableResult, DeleteItemRequest, DeleteItemResult, DeleteTableRequest, DeleteTableResult, GetItemRequest, GetItemResult, PutItemRequest, PutItemResult, QueryRequest, QueryResult, ScanRequest, ScanResult}
import com.hiya.alternator.aws1.{Aws1Table, Aws1TableWithRangeKey}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{DynamoDB, TableLike, TableWithRangeLike}
import fs2.Stream

import java.util.concurrent.{Future => JFuture}


class CatsAws1[F[_]: Async] extends DynamoDB[F, Stream[F, *], AmazonDynamoDBAsync] {
  override type BatchGetItemResponse = model.BatchGetItemResult
  override type BatchWriteItemResponse = model.BatchWriteItemResult

  private def async[Req <: AmazonWebServiceRequest, Resp](f: AsyncHandler[Req, Resp] => JFuture[Resp]): F[Resp] = {
    Async[F].async_[Resp] { cb =>
      f(new AsyncHandler[Req, Resp] {
        override def onError(exception: Exception): Unit = cb(Left(exception))
        override def onSuccess(request: Req, result: Resp): Unit = cb(Right(result))
      }): Unit
    }
  }

  override def get[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], pk: PK): F[Option[Result[V]]] =
    async(table.client.getItemAsync(Aws1Table(table).get(pk), _: AsyncHandler[GetItemRequest, GetItemResult]))
      .map(Aws1Table(table).deserialize)

  override def put[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], item: V, condition: Option[ConditionExpression[Boolean]]): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(table.client.putItemAsync(Aws1Table(table).put(item, condition), _: AsyncHandler[PutItemRequest, PutItemResult]))
          .map(_ => true)
          .recover { case _: ConditionalCheckFailedException => false }

      case None =>
        async(table.client.putItemAsync(Aws1Table(table).put(item), _: AsyncHandler[PutItemRequest, PutItemResult]))
          .map(_ => true)
    }

  override def delete[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], key: PK, condition: Option[ConditionExpression[Boolean]]): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(table.client.deleteItemAsync(Aws1Table(table).delete(key, condition), _: AsyncHandler[DeleteItemRequest, DeleteItemResult]))
          .map(_ => true)
          .recover { case _: ConditionalCheckFailedException => false }

      case None =>
        async(table.client.deleteItemAsync(Aws1Table(table).delete(key), _: AsyncHandler[DeleteItemRequest, DeleteItemResult]))
          .map(_ => true)
    }

  override def createTable(client: AmazonDynamoDBAsync, tableName: String, hashKey: String, rangeKey: Option[String], readCapacity: Long, writeCapacity: Long, attributes: List[(String, ScalarType)]): F[Unit] =
    async(
      client.createTableAsync(
        Aws1Table.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes),
        _: AsyncHandler[CreateTableRequest, CreateTableResult]
      )
    ).map(_ => ())

  override def dropTable(client: AmazonDynamoDBAsync, tableName: String): F[Unit] = {
    async(
      client.deleteTableAsync(
        Aws1Table.dropTable(tableName),
        _: AsyncHandler[DeleteTableRequest, DeleteTableResult]
      )
    ).map(_ => ())
  }


  private def scanPaginator(f: (ScanRequest, AsyncHandler[ScanRequest, ScanResult]) => JFuture[ScanResult], request: ScanRequest): Stream[F, ScanResult] = {
    Stream.unfoldLoopEval(request) { req =>
      async(f(req, _))
        .map { result =>
          result -> Option(result.getLastEvaluatedKey).map { key =>
            req.withExclusiveStartKey(key)
          }
        }
    }
  }

  override def scan[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], segment: Option[Segment]): Stream[F, Result[V]] =
    scanPaginator(table.client.scanAsync, Aws1Table(table).scan(segment))
      .flatMap(data => Stream.emits(Aws1Table(table).deserialize(data)))


  private def queryPaginator(f: (QueryRequest, AsyncHandler[QueryRequest, QueryResult]) => JFuture[QueryResult], request: QueryRequest): Stream[F, QueryResult] = {
    Stream.unfoldLoopEval(request) { req =>
      async(f(req, _))
        .map { result =>
          result -> Option(result.getLastEvaluatedKey).map { key =>
            req.withExclusiveStartKey(key)
          }
        }
    }
  }

  override def query[V, PK, RK](table: TableWithRangeLike[AmazonDynamoDBAsync, V, PK, RK], pk: PK, rk: RKCondition[RK]): Stream[F, Result[V]] =
    queryPaginator(table.client.queryAsync, Aws1TableWithRangeKey(table).query(pk, rk))
      .flatMap { data => fs2.Stream.emits(Aws1TableWithRangeKey(table).deserialize(data)) }

  override def eval[T](f: => F[T]): Stream[F, T] = Stream.eval(f)

  override def bracket[T, B](acquire: => F[T])(release: T => F[Unit])(s: T => Stream[F, B]): Stream[F, B] =
    Stream.bracket(acquire)(release).flatMap(s)

  override def evalMap[A, B](in: Stream[F, A])(f: A => F[B]): Stream[F, B] =
    in.evalMap(f)

  override def batchGet[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], keys: Seq[PK]): F[BatchGetItemResult] =
    async(table.client.batchGetItemAsync(Aws1Table(table).batchGet(keys), _: AsyncHandler[model.BatchGetItemRequest, model.BatchGetItemResult]))

  override def batchWrite[V, PK](table: TableLike[AmazonDynamoDBAsync, V, PK], values: Seq[Either[PK, V]]): F[BatchWriteItemResult] =
    async(table.client.batchWriteItemAsync(Aws1Table(table).batchWrite(values), _: AsyncHandler[model.BatchWriteItemRequest, model.BatchWriteItemResult]))
}

object CatsAws1 {
  val forIO: CatsAws1[IO] = forAsync[IO]

  def forAsync[F[_]: Async]: CatsAws1[F] = new CatsAws1[F]

  def apply[F[_]](implicit F: CatsAws1[F]): CatsAws1[F] = F
}
