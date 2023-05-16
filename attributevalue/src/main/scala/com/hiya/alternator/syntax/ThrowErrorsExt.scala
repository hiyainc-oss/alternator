package com.hiya.alternator.syntax

import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.{MonadError, Traverse}
import com.hiya.alternator.util.MonadErrorThrowable
import com.hiya.alternator.{DynamoFormat, Table}


class ThrowErrorsExt[T, F[_] : MonadErrorThrowable, M[_]: Traverse](underlying: F[M[DynamoFormat.Result[T]]]) {
  def raiseError: F[M[T]] = {
    underlying.flatMap(result =>
      MonadError[F, Throwable].fromTry(result.traverse(Table.orFail))
    )
  }
}
