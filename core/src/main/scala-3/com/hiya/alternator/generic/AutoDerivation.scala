package com.hiya.alternator.generic

import com.hiya.alternator.generic.format.DerivedDynamoFormat
import com.hiya.alternator.schema.RootDynamoFormat
import scala.deriving.Mirror

trait AutoDerivation:
  inline given autoDerive[A](using Mirror.Of[A]): RootDynamoFormat[A] =
    DerivedDynamoFormat.derive[A]
