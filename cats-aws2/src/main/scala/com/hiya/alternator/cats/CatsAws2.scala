package com.hiya.alternator.cats

import _root_.cats.effect._
import _root_.cats.syntax.all._
import com.hiya.alternator._
import com.hiya.alternator.aws2.internal.Aws2DynamoDB
import com.hiya.alternator.aws2.{Aws2DynamoDBClient, Aws2IndexOps, Aws2TableOps, Aws2TableWithRangeKeyOps}
import com.hiya.alternator.cats.internal.CatsBase
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import fs2.Stream
import software.amazon.awssdk.services.dynamodb.model.{QueryRequest, QueryResponse, ScanRequest, ScanResponse}

import java.util.concurrent.CompletableFuture

class CatsAws2[F[+_]](protected override implicit val F: Async[F])
  extends Aws2DynamoDB[F, Stream[F, *]]
  with CatsBase[F] {

  override protected def async[T](f: => CompletableFuture[T]): F[T] = {
    Async[F].fromCompletableFuture(Async[F].delay(f))
  }

  private def scanPaginator(
    f: ScanRequest => CompletableFuture[ScanResponse],
    request: ScanRequest.Builder,
    limit: Option[Int]
  ): Stream[F, ScanResponse] = {
    Stream.unfoldLoopEval[F, (ScanRequest.Builder, Option[Int]), ScanResponse](request -> limit) { case (req, limit) =>
      async(f(req.limit(limit.map(Int.box).orNull).build()))
        .map { result =>
          val newReq = limit.map(_ - result.count()) match {
            case Some(limit) if limit == 0 =>
              None
            case _ if result.lastEvaluatedKey().isEmpty =>
              None
            case limit =>
              Some(req.exclusiveStartKey(result.lastEvaluatedKey()) -> limit)
          }
          result -> newReq
        }
    }
  }

  override def scan[V, PK](
    table: TableLike[Aws2DynamoDBClient, V, PK],
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean,
    overrides: DynamoDBOverride[Client]
  ): Stream[F, Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    scanPaginator(
      table.client.client.scan,
      Aws2TableOps(table).scan(segment, condition, consistent, resolvedOverride),
      limit
    )
      .flatMap(data => Stream.emits(Aws2TableOps(table).deserialize(data)))
  }

  private def queryPaginator(
    f: QueryRequest => CompletableFuture[QueryResponse],
    request: QueryRequest.Builder,
    limit: Option[Int]
  ): Stream[F, QueryResponse] = {
    Stream.unfoldLoopEval[F, (QueryRequest.Builder, Option[Int]), QueryResponse](request -> limit) {
      case (req, limit) =>
        async(f(req.limit(limit.map(Int.box).orNull).build()))
          .map { result =>
            val newReq = limit.map(_ - result.count()) match {
              case Some(limit) if limit == 0 =>
                None
              case _ if result.lastEvaluatedKey().isEmpty =>
                None
              case limit =>
                Some(req.exclusiveStartKey(result.lastEvaluatedKey()) -> limit)
            }
            result -> newReq
          }
    }
  }

  override def query[V, PK, RK](
    table: TableWithRangeLike[Aws2DynamoDBClient, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[Boolean]] = None,
    limit: Option[Int] = None,
    consistent: Boolean = false,
    overrides: DynamoDBOverride[Client] = DynamoDBOverride.empty
  ): Stream[F, Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    queryPaginator(
      table.client.client.query,
      Aws2TableWithRangeKeyOps(table).query(pk, rk, condition, consistent, resolvedOverride),
      limit
    ).flatMap { data => fs2.Stream.emits(Aws2TableWithRangeKeyOps(table).deserialize(data)) }
  }

  override def queryPK[V, PK](
    table: Index[Aws2DynamoDBClient, V, PK],
    pk: PK,
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean,
    overrides: DynamoDBOverride[Aws2DynamoDBClient]
  ): Source[Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    queryPaginator(
      table.client.client.query,
      Aws2IndexOps(table).query(pk, condition, consistent, resolvedOverride),
      limit
    ).flatMap { data => fs2.Stream.emits(Aws2IndexOps(table).deserialize(data)) }
  }
}

object CatsAws2 {
  val forIO: CatsAws2[IO] = forAsync[IO]

  def forAsync[F[+_]: Async]: CatsAws2[F] = new CatsAws2[F]

  def apply[F[+_]](implicit F: CatsAws2[F]): CatsAws2[F] = F
}
