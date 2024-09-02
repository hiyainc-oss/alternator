package com.hiya.alternator.akka

import java.util.concurrent.CompletableFuture
import scala.concurrent.{ExecutionContext, Future}
import scala.compat.java8.FutureConverters

private[akka] object JdkCompat {
  implicit lazy val parasitic: ExecutionContext = {
    // The backport is present in akka, so we will just use it by reflection
    // It probably will not change, as it is a stable internal api
    val q = akka.dispatch.ExecutionContexts
    val clazz = q.getClass
    val field = clazz.getDeclaredField("parasitic")
    field.setAccessible(true)
    val ret = field.get(q).asInstanceOf[ExecutionContext]
    field.setAccessible(false)
    ret
  }

  implicit class CompletionStage[T](val cs: CompletableFuture[T]) extends AnyVal {
    @inline def asScala: Future[T] = FutureConverters.toScala(cs)
  }
}
