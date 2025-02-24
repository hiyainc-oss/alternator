package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator.akka.internal.AkkaBase
import com.hiya.alternator.aws1.internal.Aws1DynamoDB
import com.hiya.alternator.aws1.{Aws1TableOps, Aws1TableWithRangeKeyOps}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}
import com.hiya.alternator.{Table, TableWithRange}

import java.util.concurrent.{Future => JFuture}
import scala.concurrent.{ExecutionContext, Future, Promise}

class AkkaAws1 private (override implicit val system: ActorSystem, override implicit val workerEc: ExecutionContext)
  extends Aws1DynamoDB[Future, akka.stream.scaladsl.Source[*, NotUsed]]
  with AkkaBase {

  override protected def async[Req <: AmazonWebServiceRequest, Resp](
    f: AsyncHandler[Req, Resp] => JFuture[Resp]
  ): Future[Resp] = AkkaAws1.async(f)

  private def scanPaginator(
    f: (ScanRequest, AsyncHandler[ScanRequest, ScanResult]) => JFuture[ScanResult],
    request: ScanRequest,
    limit: Option[Int]
  ): Source[ScanResult] = {
    akka.stream.scaladsl.Source.unfoldAsync[Option[(ScanRequest, Option[Int])], ScanResult](Some(request -> limit)) {
      case None => Future.successful(None)
      case Some((req, limit)) =>
        async(f(req.withLimit(limit.map(Int.box).orNull), _))
          .map { result =>
            val newReq = limit.map(_ - result.getCount) match {
              case Some(limit) if limit == 0 =>
                None
              case newLimit =>
                Option(result.getLastEvaluatedKey).map { key =>
                  req.withExclusiveStartKey(key) -> newLimit
                }
            }
            Some(newReq -> result)
          }
    }
  }

  override def scan[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean
  ): Source[Result[V]] = {
    scanPaginator(table.client.scanAsync, Aws1TableOps(table).scan(segment, condition, consistent), limit)
      .mapConcat(data => Aws1TableOps(table).deserialize(data))
  }

  private def queryPaginator(
    f: (QueryRequest, AsyncHandler[QueryRequest, QueryResult]) => JFuture[QueryResult],
    request: QueryRequest,
    limit: Option[Int]
  ): Source[QueryResult] = {
    akka.stream.scaladsl.Source.unfoldAsync[Option[(QueryRequest, Option[Int])], QueryResult](Some(request -> limit)) {
      case None => Future.successful(None)
      case Some((req, limit)) =>
        async(f(req.withLimit(limit.map(Int.box).orNull), _))
          .map { result =>
            val newReq = limit.map(_ - result.getCount) match {
              case Some(limit) if limit == 0 =>
                None
              case limit =>
                Option(result.getLastEvaluatedKey).map { key =>
                  req.withExclusiveStartKey(key) -> limit
                }
            }
            Some(newReq -> result)
          }
    }
  }

  override def query[V, PK, RK](
    table: TableWithRange[AmazonDynamoDBAsync, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean
  ): Source[Result[V]] = {
    queryPaginator(table.client.queryAsync, Aws1TableWithRangeKeyOps(table).query(pk, rk, condition, consistent), limit)
      .mapConcat { data => Aws1TableWithRangeKeyOps(table).deserialize(data) }
  }
}

object AkkaAws1 {
  def apply()(implicit system: ClassicActorSystemProvider): AkkaAws1 = {
    apply(system.classicSystem.dispatcher)
  }

  def apply(ec: ExecutionContext)(implicit system: ClassicActorSystemProvider): AkkaAws1 = {
    new AkkaAws1()(system.classicSystem, ec)
  }

  @inline private[akka] def async[Req <: AmazonWebServiceRequest, Resp](
    f: AsyncHandler[Req, Resp] => JFuture[Resp]
  ): Future[Resp] = {
    val p = Promise[Resp]()

    val _ = f(new AsyncHandler[Req, Resp] {
      override def onError(exception: Exception): Unit = p.failure(exception)
      override def onSuccess(request: Req, result: Resp): Unit = p.success(result)
    })

    p.future
  }
}
