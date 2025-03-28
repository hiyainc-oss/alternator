package com.hiya.alternator.akka.internal

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl
import akka.stream.scaladsl.{Sink, Source}
import cats.Traverse
import cats.syntax.all._
import com.hiya.alternator.DynamoDB
import com.hiya.alternator.akka.JdkCompat

import scala.concurrent.{ExecutionContext, Future}

trait AkkaBase {
  this: DynamoDB[Future] =>

  override type Source[T] = scaladsl.Source[T, NotUsed]

  protected implicit val system: ActorSystem
  protected implicit val workerEc: ExecutionContext

  def eval[T](f: => Future[T]): Source[T] = Source.lazyFuture(() => f)
  def evalMap[A, B](in: Source[A])(f: A => Future[B]): Source[B] = in.mapAsync(1)(f)
  def bracket[T, B](
    acquire: => Future[T]
  )(release: T => Future[Unit])(s: T => Source[B]): Source[B] = {
    Source.lazyFuture(() => acquire).flatMapConcat { t =>
      s(t).watchTermination() { (_, done) =>
        done.onComplete(_ => release(t))(JdkCompat.parasitic)
      }
    }
  }

  def toSeq[T](value: Source[T]): Future[Seq[T]] = value.runWith(Sink.seq)

  def parTraverse[M[_]: Traverse, A, B](values: M[A])(f: A => Future[B]): Future[M[B]] =
    values.map(f).sequence
}
