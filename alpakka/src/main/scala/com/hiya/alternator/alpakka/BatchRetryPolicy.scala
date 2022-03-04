package com.hiya.alternator.alpakka

import scala.concurrent.duration._


trait BatchRetryPolicy {
  def delayForError(retryCount: Int): Option[FiniteDuration]
  def delayForThrottle(retryCount: Int): Option[FiniteDuration]
  def delayForUnprocessed(retryCount: Int): Option[FiniteDuration]
}

object BatchRetryPolicy {
  val defaultBackoffStrategy: BackoffStrategy =
    BackoffStrategy.FullJitter(20.seconds, 25.millis)

  final case class DefaultBatchRetryPolicy(
    awsHasRetry: Boolean = true,
    maxRetries: Int = 8,
    throttleBackoff: BackoffStrategy = defaultBackoffStrategy,
    errorBackoff: BackoffStrategy = defaultBackoffStrategy
  ) extends BatchRetryPolicy {
    override def delayForError(retryCount: Int): Option[FiniteDuration] = {
      if (!awsHasRetry && retryCount < maxRetries) Some(errorBackoff.getRetry(retryCount))
      else None
    }

    override def delayForThrottle(retryCount: Int): Option[FiniteDuration] = {
      if (!awsHasRetry && retryCount < maxRetries) Some(throttleBackoff.getRetry(retryCount))
      else None
    }

    override def delayForUnprocessed(retryCount: Int): Option[FiniteDuration] = {
      if (retryCount < maxRetries) Some(throttleBackoff.getRetry(retryCount))
      else None
    }
  }
}
