package com.hiya.alternator.generic

import com.hiya.alternator.schema.RootDynamoFormat
import com.hiya.alternator.generic.format.DerivedDynamoFormat
import com.hiya.alternator.generic.util.{ExportMacros, Exported}

trait AutoDerivation {

  implicit def exportDecoder[A]: Exported[RootDynamoFormat[A]] =
    macro ExportMacros.exportDynamoFormat[DerivedDynamoFormat, A]
}
