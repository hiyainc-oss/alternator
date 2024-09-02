package com.hiya.alternator.schema

sealed trait ScalarType
object ScalarType {
  object String extends ScalarType
  object Binary extends ScalarType
  object Numeric extends ScalarType
}
