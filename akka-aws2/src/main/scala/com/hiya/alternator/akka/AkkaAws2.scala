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

  override def scan[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]]
  ): Source[Result[V], NotUsed] =
    Source
      .fromPublisher(table.client.scanPaginator(Aws2Table(table).scan(segment, condition).build()))
      .mapConcat(data => Aws2Table(table).deserialize(data))

  override def query[V, PK, RK](
    table: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]]
  ): Source[Result[V], NotUsed] = {
    Source
      .fromPublisher(table.client.queryPaginator(Aws2TableWithRangeKey(table).query(pk, rk, condition).build()))
      .mapConcat(data => Aws2TableWithRangeKey(table).deserialize(data))
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
