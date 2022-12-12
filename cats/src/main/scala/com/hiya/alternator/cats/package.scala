package com.hiya.alternator

import _root_.cats.Traverse
import com.hiya.alternator.cats.internal.ThrowErrorsExt
import com.hiya.alternator.util.MonadErrorThrowable


package object cats {
  implicit def toTry[T, F[_] : MonadErrorThrowable, M[_] : Traverse](underlying: F[M[DynamoFormat.Result[T]]]): ThrowErrorsExt[T, F, M] =
    new ThrowErrorsExt[T, F, M](underlying)
}
