package com.hiya.alternator.cats.internal

import cats.Traverse
import cats.effect.Async
import cats.effect.implicits._
import cats.syntax.all._
import com.hiya.alternator.{DynamoDB, DynamoDBSource}
import fs2.Stream

trait CatsBase[F[_]] {
  this: DynamoDB[F] with DynamoDBSource =>

  override type Source[T] = Stream[Monad, T]

  protected implicit val F: Async[F]

  override def parTraverse[M[_]: Traverse, A, B](values: M[A])(f: A => F[B]): F[M[B]] =
    values.parTraverse(f)

  override def eval[T](f: => Monad[T]): Source[T] = Stream.eval(f)

  override def bracket[T, B](acquire: => F[T])(release: T => F[Unit])(s: T => Stream[F, B]): Stream[F, B] =
    Stream.bracket(acquire)(release).flatMap(s)

  override def evalMap[A, B](in: Stream[F, A])(f: A => F[B]): Stream[F, B] =
    in.evalMap(f)

  override def toSeq[T](value: Stream[F, T]): F[Vector[T]] = value.compile.toVector
}
