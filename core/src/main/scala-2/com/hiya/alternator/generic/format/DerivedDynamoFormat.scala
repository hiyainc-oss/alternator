package com.hiya.alternator.generic.format

import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{AttributeValue, RootDynamoFormat}
import shapeless.{LabelledGeneric, Lazy}

trait DerivedDynamoFormat[T] extends RootDynamoFormat[T]

object DerivedDynamoFormat {
  implicit def deriveDecoder[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    decode: Lazy[ReprDynamoFormat[R]]
  ): DerivedDynamoFormat[A] = new DerivedDynamoFormat[A] {

    final override def writeFields[AV: AttributeValue](value: A): java.util.Map[String, AV] =
      decode.value.writeFields(gen.to(value))

    final override def readFields[AV: AttributeValue](av: java.util.Map[String, AV]): Result[A] =
      decode.value.readFields(av).map(gen.from)
  }

}
