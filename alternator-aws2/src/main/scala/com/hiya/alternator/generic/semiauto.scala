package com.hiya.alternator.generic

import com.hiya.alternator.CompoundDynamoFormat
import com.hiya.alternator.generic.format.DerivedDynamoFormat
import shapeless.Lazy

object semiauto {
  final def deriveCompound[A](implicit decode: Lazy[DerivedDynamoFormat[A]]): CompoundDynamoFormat[A] = decode.value
}
