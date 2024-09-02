package com.hiya.alternator

import scala.concurrent.duration.FiniteDuration

case class BatchTimeout(timeout: FiniteDuration) extends AnyVal
