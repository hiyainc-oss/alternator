package com.hiya.alternator

import cats.MonadThrow
import cats.syntax.flatMap._
import cats.syntax.functor._

package object internal {
  implicit class OptApp[T](underlying: T) {

    def optApp[A](f: T => A => T): Option[A] => T = {
      case Some(a) => f(underlying)(a)
      case None => underlying
    }
  }

  implicit class OptAppF[F[_]: MonadThrow, T](underlying: F[T]) {

    def optApp[A](f: T => A => T): Option[A] => F[T] = {
      case Some(a) => underlying.map(f(_)(a))
      case None => underlying
    }

    def optAppF[A](f: T => A => T): Option[F[A]] => F[T] = {
      case Some(a) =>
        for {
          t <- underlying
          p <- a
        } yield f(t)(p)

      case None => underlying
    }

  }
}
