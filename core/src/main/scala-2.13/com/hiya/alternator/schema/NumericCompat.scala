package com.hiya.alternator.schema

trait NumericCompat {
  type Numeric[T] = scala.math.Numeric[T]
  val Numeric = scala.math.Numeric
}
