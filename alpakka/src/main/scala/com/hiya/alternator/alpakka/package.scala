package com.hiya.alternator

import akka.actor.typed.ActorRef
import cats.Traverse
import com.hiya.alternator.alpakka.internal.{ShutdownExts, ThrowErrorsExt}
import com.hiya.alternator.util.MonadErrorThrowable

package object alpakka {
  implicit def shutdownExts[T](actorRef: ActorRef[T])(implicit T: ShutdownExts.Support[T]): ShutdownExts[T] = T(actorRef)

  implicit def toTry[T, F[_]: MonadErrorThrowable, M[_]: Traverse](underlying: F[M[DynamoFormat.Result[T]]]): ThrowErrorsExt[T, F, M] =
    new ThrowErrorsExt[T, F, M](underlying)

}
