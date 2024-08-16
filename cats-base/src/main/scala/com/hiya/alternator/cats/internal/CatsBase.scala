package com.hiya.alternator.cats.internal

import cats.Traverse
import cats.effect.Async
import cats.effect.implicits._
import cats.syntax.all._
import fs2.Stream

trait CatsBase[F[_]] {
  protected implicit val F: Async[F]

  def parTraverse[M[_]: Traverse, A, B](values: M[A])(f: A => F[B]): F[M[B]] =
    values.parTraverse(f)

  def eval[T](f: => F[T]): Stream[F, T] = Stream.eval(f)

  def bracket[T, B](acquire: => F[T])(release: T => F[Unit])(s: T => Stream[F, B]): Stream[F, B] =
    Stream.bracket(acquire)(release).flatMap(s)

  def evalMap[A, B](in: Stream[F, A])(f: A => F[B]): Stream[F, B] =
    in.evalMap(f)

  def toSeq[T](value: Stream[F, T]): F[Vector[T]] = value.compile.toVector
}
