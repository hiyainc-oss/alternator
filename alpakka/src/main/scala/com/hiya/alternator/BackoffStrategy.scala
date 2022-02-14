package com.hiya.alternator

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

trait BackoffStrategy {
  def getRetry(retry: Int): FiniteDuration
}

object BackoffStrategy {
  private val retryMax = 30

  final case class Static(delay: FiniteDuration) extends BackoffStrategy {
    override def getRetry(retry: Int): FiniteDuration = delay
  }

  final case class FullJitter(cap: FiniteDuration, base: FiniteDuration) extends BackoffStrategy {
    private val baseMs = base.toMillis
    private val capMs = cap.toMillis

    assert(baseMs >=0 && baseMs < Integer.MAX_VALUE)

    override def getRetry(retry: Int): FiniteDuration = {
      val maxDelay = capMs.min(baseMs << retry.min(retryMax))
      FiniteDuration(Random.between(0, maxDelay), TimeUnit.MILLISECONDS)
    }
  }

  final case class EqualJitter(cap: FiniteDuration, base: FiniteDuration) extends BackoffStrategy {
    private val baseMs = base.toMillis
    private val capMs = cap.toMillis

    assert(baseMs >=0 && baseMs < Integer.MAX_VALUE)

    override def getRetry(retry: Int): FiniteDuration = {
      val maxDelay = capMs.min(baseMs << retry.min(retryMax))
      FiniteDuration(maxDelay / 2 + Random.between(0, maxDelay / 2), TimeUnit.MILLISECONDS)
    }
  }


}
