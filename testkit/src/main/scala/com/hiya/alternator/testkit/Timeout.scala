package com.hiya.alternator.testkit

import scala.concurrent.duration.FiniteDuration

final case class Timeout(value: FiniteDuration) extends AnyVal

object Timeout {
  implicit def timeout(value: FiniteDuration): Timeout = Timeout(value)
}
