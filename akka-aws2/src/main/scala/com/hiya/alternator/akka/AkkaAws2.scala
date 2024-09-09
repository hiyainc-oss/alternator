package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.stream.scaladsl.Source
import cats.instances.future._
import com.hiya.alternator.akka.internal.AkkaBase
import com.hiya.alternator.aws2.internal.Aws2DynamoDB
import com.hiya.alternator.aws2.{Aws2Table, Aws2TableWithRangeKey}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{TableLike, TableWithRangeKeyLike}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{QueryRequest, QueryResponse, ScanRequest, ScanResponse}

import java.util.concurrent.{CompletableFuture, CompletionException}
import scala.concurrent.{ExecutionContext, Future}

class AkkaAws2 private (override implicit val system: ActorSystem, override implicit val workerEc: ExecutionContext)
  extends Aws2DynamoDB[Future, Source[*, NotUsed]]
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
  ): Source[ScanResponse, NotUsed] = {
    Source.unfoldAsync[Option[(ScanRequest.Builder, Option[Int])], ScanResponse](Some(request -> limit)) {
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
    table: TableLike[DynamoDbAsyncClient, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int] = None,
    consistent: Boolean = false
  ): Source[Result[V], NotUsed] =
    scanPaginator(
      table.client.scan,
      Aws2Table(table).scan(segment, condition, consistent),
      limit
    ).mapConcat(data => Aws2Table(table).deserialize(data))

  private def queryPaginator(
    f: QueryRequest => CompletableFuture[QueryResponse],
    request: QueryRequest.Builder,
    limit: Option[Int]
  ): Source[QueryResponse, NotUsed] = {
    Source.unfoldAsync[Option[(QueryRequest.Builder, Option[Int])], QueryResponse](Some(request -> limit)) {
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
    table: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int] = None,
    consistent: Boolean = false
  ): Source[Result[V], NotUsed] =
    queryPaginator(
      table.client.query,
      Aws2TableWithRangeKey(table)
        .query(pk, rk, condition, consistent)
        .limit(limit.map(Int.box).orNull),
      limit
    ).mapConcat(data => Aws2TableWithRangeKey(table).deserialize(data))
}

object AkkaAws2 {
  def apply()(implicit system: ClassicActorSystemProvider): AkkaAws2 = {
    apply(system.classicSystem.dispatcher)
  }

  def apply(ec: ExecutionContext)(implicit system: ClassicActorSystemProvider): AkkaAws2 = {
    new AkkaAws2()(system.classicSystem, ec)
  }
}
