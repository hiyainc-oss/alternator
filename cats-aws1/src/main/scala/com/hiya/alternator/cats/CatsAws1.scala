package com.hiya.alternator.cats

import cats.effect.{Async, IO}
import cats.syntax.all._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model.{QueryRequest, QueryResult, ScanRequest, ScanResult}
import com.hiya.alternator.aws1.internal.Aws1DynamoDB
import com.hiya.alternator.aws1.{Aws1Table, Aws1TableWithRangeKey}
import com.hiya.alternator.cats.internal.CatsBase
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{TableLike, TableWithRangeKeyLike}
import fs2.Stream

import java.util.concurrent.{Future => JFuture}

class CatsAws1[F[+_]](protected override implicit val F: Async[F])
  extends Aws1DynamoDB[F, Stream[F, *]]
  with CatsBase[F] {

  protected override def async[Req <: AmazonWebServiceRequest, Resp](
    f: AsyncHandler[Req, Resp] => JFuture[Resp]
  ): F[Resp] = {
    Async[F].async_[Resp] { cb =>
      val _ = f(new AsyncHandler[Req, Resp] {
        override def onError(exception: Exception): Unit = cb(Left(exception))
        override def onSuccess(request: Req, result: Resp): Unit = cb(Right(result))
      })
    }
  }

  private def scanPaginator(
    f: (ScanRequest, AsyncHandler[ScanRequest, ScanResult]) => JFuture[ScanResult],
    request: ScanRequest
  ): Stream[F, ScanResult] = {
    Stream.unfoldLoopEval(request) { req =>
      async(f(req, _))
        .map { result =>
          result -> Option(result.getLastEvaluatedKey).map { key =>
            req.withExclusiveStartKey(key)
          }
        }
    }
  }

  override def scan[V, PK](
    table: TableLike[AmazonDynamoDBAsync, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean
  ): Stream[F, Result[V]] =
    scanPaginator(table.client.scanAsync, Aws1Table(table).scan(segment, condition, limit, consistent))
      .flatMap(data => Stream.emits(Aws1Table(table).deserialize(data)))

  private def queryPaginator(
    f: (QueryRequest, AsyncHandler[QueryRequest, QueryResult]) => JFuture[QueryResult],
    request: QueryRequest
  ): Stream[F, QueryResult] = {
    Stream.unfoldLoopEval(request) { req =>
      async(f(req, _))
        .map { result =>
          result -> Option(result.getLastEvaluatedKey).map { key =>
            req.withExclusiveStartKey(key)
          }
        }
    }
  }

  override def query[V, PK, RK](
    table: TableWithRangeKeyLike[AmazonDynamoDBAsync, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean
  ): Stream[F, Result[V]] =
    queryPaginator(table.client.queryAsync, Aws1TableWithRangeKey(table).query(pk, rk, condition, limit, consistent))
      .flatMap { data => fs2.Stream.emits(Aws1TableWithRangeKey(table).deserialize(data)) }
}

object CatsAws1 {
  val forIO: CatsAws1[IO] = forAsync[IO]

  def forAsync[F[+_]: Async]: CatsAws1[F] = new CatsAws1[F]

  def apply[F[+_]](implicit F: CatsAws1[F]): CatsAws1[F] = F
}
