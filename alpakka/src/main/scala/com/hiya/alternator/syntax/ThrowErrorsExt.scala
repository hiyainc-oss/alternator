package com.hiya.alternator.syntax

import cats.syntax.all._
import cats.{MonadError, Traverse}
import com.hiya.alternator.util.MonadErrorThrowable

import scala.util.{Failure, Success, Try}


class ThrowErrorsExt[T, F[_] : MonadErrorThrowable, M[_]: Traverse](underlying: F[M[Try[T]]]) {
  def throwErrors: F[M[T]] =
    underlying.flatMap(_.traverse {
      case Success(value) => MonadError[F, Throwable].pure(value)
      case Failure(value) => MonadError[F, Throwable].raiseError(value)
    })
}
