package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import cats.MonadThrow
import com.hiya.alternator.{DynamoDB, DynamoDBTestBase}
import com.hiya.alternator.aws2._
import com.hiya.alternator.testkit.LocalDynamoDB

import scala.concurrent.{Await, Future}
import com.hiya.alternator.aws2.internal.Aws2DynamoDBClient
import com.hiya.alternator.DynamoDBClient

class AkkaAws2DBTests extends DynamoDBTestBase[Future, Source[*, NotUsed], Aws2DynamoDBClient] {
  private implicit lazy val system: ActorSystem = ActorSystem()
  import system.dispatcher

  override protected lazy val client: Aws2DynamoDBClient = LocalDynamoDB.client()
  override protected implicit lazy val DB: DynamoDB.Aux[Future, Source[*, NotUsed], Aws2DynamoDBClient] = AkkaAws2()
  override protected implicit lazy val monadF: MonadThrow[Future] = cats.instances.future.catsStdInstancesForFuture
  override protected implicit lazy val monadS: MonadThrow[Source[*, NotUsed]] = new MonadThrow[Source[*, NotUsed]] {
    override def pure[A](x: A): Source[A, NotUsed] = Source.single(x)
    override def flatMap[A, B](fa: Source[A, NotUsed])(f: A => Source[B, NotUsed]): Source[B, NotUsed] =
      fa.flatMapConcat(f)

    override def raiseError[A](e: Throwable): Source[A, NotUsed] = Source.failed(e)
    override def handleErrorWith[A](fa: Source[A, NotUsed])(f: Throwable => Source[A, NotUsed]): Source[A, NotUsed] =
      fa.recoverWithRetries(1, { case _ => f(new Exception("error")) })

    override def tailRecM[A, B](a: A)(f: A => Source[Either[A, B], NotUsed]): Source[B, NotUsed] = ???
  }
  override implicit protected def hasOverride: DynamoDBClient.HasOverride[Aws2DynamoDBClient, _] = DynamoDBClient.HasOverride.forNothing

  override protected def eval[T](body: Future[T]): T =
    Await.result(body, scala.concurrent.duration.Duration.Inf)
  override protected def list[T](body: Source[T, NotUsed]): Future[List[T]] =
    body.runWith(Sink.seq).map(_.toList)
}
