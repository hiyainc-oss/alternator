package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import cats.instances.future._
import cats.syntax.all._
import com.hiya.alternator.akka.internal.AkkaBase
import com.hiya.alternator.aws2.internal.Aws2DynamoDB
import com.hiya.alternator.aws2.{Aws2DynamoDBClient, Aws2IndexOps, Aws2TableLikeOps, Aws2TableWithRangeLikeOps}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{DynamoDBOverride, Index, TableLike, TableWithRangeLike}
import software.amazon.awssdk.services.dynamodb.model.{QueryRequest, QueryResponse, ScanRequest, ScanResponse}

import java.util.concurrent.{CompletableFuture, CompletionException}
import scala.concurrent.{ExecutionContext, Future}

class AkkaAws2 private (override implicit val system: ActorSystem, override implicit val workerEc: ExecutionContext)
  extends Aws2DynamoDB[Future, akka.stream.scaladsl.Source[*, NotUsed]]
  with AkkaBase {

  override protected def async[T](f: => CompletableFuture[T]): Future[T] = {
    JdkCompat
      .CompletionStage(f)
      .asScala
      .recoverWith { case ex: CompletionException => Future.failed(ex.getCause) }
  }

  private def scanPaginator(
    f: ScanRequest => CompletableFuture[ScanResponse],
    request: ScanRequest.Builder,
    limit: Option[Int]
  ): Source[ScanResponse] = {
    akka.stream.scaladsl.Source
      .unfoldAsync[Option[(ScanRequest.Builder, Option[Int])], ScanResponse](Some(request -> limit)) {
        case None => Future.successful(None)
        case Some((req, limit)) =>
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
              Some(newReq -> result)
            }
      }
  }

  override def scan[V, PK](
    table: TableLike[Aws2DynamoDBClient, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int] = None,
    consistent: Boolean = false,
    overrides: DynamoDBOverride[Client] = DynamoDBOverride.empty
  ): Source[Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    scanPaginator(
      table.client.client.scan,
      Aws2TableLikeOps(table).scan(segment, condition, consistent, resolvedOverride),
      limit
    ).mapConcat(data => Aws2TableLikeOps(table).deserialize(data))
  }

  private def queryPaginator(
    f: QueryRequest => CompletableFuture[QueryResponse],
    request: QueryRequest.Builder,
    limit: Option[Int]
  ): Source[QueryResponse] = {
    akka.stream.scaladsl.Source
      .unfoldAsync[Option[(QueryRequest.Builder, Option[Int])], QueryResponse](Some(request -> limit)) {
        case None => Future.successful(None)
        case Some((req, limit)) =>
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
              Some(newReq -> result)
            }
      }
  }

  override def query[V, PK, RK](
    table: TableWithRangeLike[Aws2DynamoDBClient, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int] = None,
    consistent: Boolean = false,
    overrides: DynamoDBOverride[Client] = DynamoDBOverride.empty
  ): Source[Result[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    queryPaginator(
      table.client.client.query,
      Aws2TableWithRangeLikeOps(table)
        .query(pk, rk, condition, consistent, resolvedOverride)
        .limit(limit.map(Int.box).orNull),
      limit
    ).mapConcat(data => Aws2TableWithRangeLikeOps(table).deserialize(data))
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
      Aws2IndexOps(table)
        .query(pk, condition, consistent, resolvedOverride)
        .limit(limit.map(Int.box).orNull),
      limit
    ).mapConcat(data => Aws2IndexOps(table).deserialize(data))
  }
}

object AkkaAws2 {
  def apply()(implicit system: ClassicActorSystemProvider): AkkaAws2 = {
    apply(system.classicSystem.dispatcher)
  }

  def apply(ec: ExecutionContext)(implicit system: ClassicActorSystemProvider): AkkaAws2 = {
    new AkkaAws2()(system.classicSystem, ec)
  }
}
