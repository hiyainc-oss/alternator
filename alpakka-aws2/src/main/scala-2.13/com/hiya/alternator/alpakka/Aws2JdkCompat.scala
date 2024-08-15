package com.hiya.alternator.alpakka

import java.util.concurrent.CompletableFuture
import scala.concurrent.{ExecutionContext, Future}

private[alpakka] object Aws2JdkCompat {
  implicit lazy val parasitic: ExecutionContext = ExecutionContext.parasitic

  implicit class CompletionStage[T](val cs: CompletableFuture[T]) extends AnyVal {
    @inline def asScala: Future[T] = scala.jdk.FutureConverters.CompletionStageOps(cs).asScala
  }
}
