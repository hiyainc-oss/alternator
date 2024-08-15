package com.hiya.alternator.alpakka

import com.hiya.alternator.aws2

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

trait BackoffStrategy {
  def getRetry(retry: Int): FiniteDuration
}

object BackoffStrategy {
  // Not a really good random
  private def random(upper: Long): Long = {
    Math.abs(Random.nextLong()) % upper
  }

  private val retryMax = 30

  final case class Static(delay: FiniteDuration) extends BackoffStrategy {
    override def getRetry(retry: Int): FiniteDuration = delay
  }

  final case class FullJitter(cap: FiniteDuration, base: FiniteDuration) extends BackoffStrategy {
    private val baseMs = base.toMillis
    private val capMs = cap.toMillis

    require(baseMs >= 0, "base delay should not be negative")
    require(baseMs < Integer.MAX_VALUE, "base delay should be less than 2^31 milliseconds")

    override def getRetry(retry: Int): FiniteDuration = {
      val maxDelay = capMs.min(baseMs << retry.min(retryMax))
      FiniteDuration(random(maxDelay), TimeUnit.MILLISECONDS)
    }
  }

  final case class EqualJitter(cap: FiniteDuration, base: FiniteDuration) extends BackoffStrategy {
    private val baseMs = base.toMillis
    private val capMs = cap.toMillis

    assert(baseMs >= 0 && baseMs < Integer.MAX_VALUE)

    override def getRetry(retry: Int): FiniteDuration = {
      val maxDelay = capMs.min(baseMs << retry.min(retryMax))
      FiniteDuration(maxDelay / 2 + random(maxDelay / 2), TimeUnit.MILLISECONDS)
    }
  }
}
