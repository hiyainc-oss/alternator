package com.hiya.alternator.generic

import com.hiya.alternator.CompoundDynamoFormat
import com.hiya.alternator.generic.format.DerivedDynamoFormat
import com.hiya.alternator.generic.util.{ExportMacros, Exported}

trait AutoDerivation {

  implicit def exportDecoder[A]: Exported[CompoundDynamoFormat[A]] =
    macro ExportMacros.exportDynamoFormat[DerivedDynamoFormat, A]
}
