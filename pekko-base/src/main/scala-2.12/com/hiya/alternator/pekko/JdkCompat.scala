package com.hiya.alternator.pekko

import java.util.concurrent.CompletableFuture
import scala.concurrent.{ExecutionContext, Future}
import scala.compat.java8.FutureConverters

private[pekko] object JdkCompat {
  // Scala 2.12 doesn't have ExecutionContext.parasitic in stdlib
  // Provide a minimal parasitic implementation that executes on current thread
  implicit lazy val parasitic: ExecutionContext = new ExecutionContext {
    override def execute(runnable: Runnable): Unit = runnable.run()
    override def reportFailure(cause: Throwable): Unit =
      ExecutionContext.defaultReporter(cause)
  }

  implicit class CompletionStage[T](val cs: CompletableFuture[T]) extends AnyVal {
    @inline def asScala: Future[T] = FutureConverters.toScala(cs)
  }
}
