package com.hiya.alternator.cats

import cats.effect.{Async, IO}
import cats.syntax.all._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.model.{QueryRequest, QueryResult, ScanRequest, ScanResult}
import com.hiya.alternator.aws1.internal.Aws1DynamoDB
import com.hiya.alternator.aws1.{Aws1DynamoDBClient, Aws1TableOps, Aws1TableWithRangeKeyOps}
import com.hiya.alternator.cats.internal.CatsBase
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{Table, TableWithRange}
import fs2.Stream

import java.util.concurrent.{Future => JFuture}
import com.hiya.alternator.DynamoDBOverride

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
    request: ScanRequest,
    limit: Option[Int]
  ): Stream[F, ScanResult] = {
    Stream.unfoldLoopEval[F, (ScanRequest, Option[Int]), ScanResult](request -> limit) { case (req, limit) =>
      async(f(req.withLimit(limit.map(Int.box).orNull), _))
        .map { result =>
          val newReq = limit.map(_ - result.getCount) match {
            case Some(limit) if limit == 0 =>
              None
            case limit =>
              Option(result.getLastEvaluatedKey).map { lastEvaluatedKey =>
                req.withExclusiveStartKey(lastEvaluatedKey) -> limit
              }
          }
          result -> newReq
        }
    }
  }

  override def scan[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Aws1DynamoDBClient, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean,
    overrides: O = DynamoDBOverride.Empty
  ): Stream[F, Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides).apply(table.client)
    scanPaginator(
      table.client.underlying.scanAsync,
      Aws1TableOps(table).scan(segment, condition, consistent, resolvedOverride),
      limit
    )
      .flatMap(data => Stream.emits(Aws1TableOps(table).deserialize(data)))
  }

  private def queryPaginator(
    f: (QueryRequest, AsyncHandler[QueryRequest, QueryResult]) => JFuture[QueryResult],
    request: QueryRequest,
    limit: Option[Int]
  ): Stream[F, QueryResult] = {
    Stream.unfoldLoopEval[F, (QueryRequest, Option[Int]), QueryResult](request -> limit) { case (req, limit) =>
      async(f(req.withLimit(limit.map(Int.box).orNull), _))
        .map { result =>
          val newReq = limit.map(_ - result.getCount) match {
            case Some(limit) if limit == 0 =>
              None
            case limit =>
              Option(result.getLastEvaluatedKey).map { lastEvaluatedKey =>
                req.withExclusiveStartKey(lastEvaluatedKey) -> limit
              }
          }
          result -> newReq
        }
    }
  }

  override def query[V, PK, RK, O: DynamoDBOverride[Client, *]](
    table: TableWithRange[Aws1DynamoDBClient, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean,
    overrides: O = DynamoDBOverride.Empty
  ): Stream[F, Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides).apply(table.client)
    queryPaginator(
      table.client.underlying.queryAsync,
      Aws1TableWithRangeKeyOps(table).query(pk, rk, condition, consistent, resolvedOverride),
      limit
    )
      .flatMap { data => fs2.Stream.emits(Aws1TableWithRangeKeyOps(table).deserialize(data)) }
  }
}

object CatsAws1 {
  val forIO: CatsAws1[IO] = forAsync[IO]

  def forAsync[F[+_]: Async]: CatsAws1[F] = new CatsAws1[F]

  def apply[F[+_]](implicit F: CatsAws1[F]): CatsAws1[F] = F
}
