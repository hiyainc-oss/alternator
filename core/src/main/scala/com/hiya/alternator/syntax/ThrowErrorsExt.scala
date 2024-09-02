package com.hiya.alternator.syntax

import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.{MonadError, MonadThrow, Traverse}
import com.hiya.alternator.Table
import com.hiya.alternator.schema.DynamoFormat

class ThrowErrorsExt[T, F[_]: MonadThrow, M[_]: Traverse](underlying: F[M[DynamoFormat.Result[T]]]) {
  def raiseError: F[M[T]] = {
    underlying.flatMap(result => MonadError[F, Throwable].fromTry(result.traverse(Table.orFail)))
  }
}

class ThrowErrorsExt2[T, F[_]: MonadThrow](underlying: F[DynamoFormat.Result[T]]) {
  def raiseError: F[T] = {
    underlying.flatMap(result => MonadError[F, Throwable].fromTry(Table.orFail(result)))
  }
}
