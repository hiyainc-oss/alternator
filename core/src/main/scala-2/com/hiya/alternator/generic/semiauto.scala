package com.hiya.alternator.generic

import com.hiya.alternator.generic.format.DerivedDynamoFormat
import com.hiya.alternator.schema.RootDynamoFormat
import shapeless.Lazy

object semiauto {
  final def derive[A](implicit decode: Lazy[DerivedDynamoFormat[A]]): RootDynamoFormat[A] = decode.value
}
