package com.hiya.alternator.generic.format

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.hiya.alternator.CompoundDynamoFormat

import java.util
import com.hiya.alternator.DynamoFormat.Result
import shapeless.{LabelledGeneric, Lazy}


trait DerivedDynamoFormat[T] extends CompoundDynamoFormat[T]

object DerivedDynamoFormat {
  implicit def deriveDecoder[A, R](
    implicit
    gen: LabelledGeneric.Aux[A, R],
    decode: Lazy[ReprDynamoFormat[R]]
  ): DerivedDynamoFormat[A] = new DerivedDynamoFormat[A] {

    final override def writeFields(value: A): util.Map[String, AttributeValue] =
      decode.value.writeFields(gen.to(value))

    final override def readFields(av: util.Map[String, AttributeValue]): Result[A] =
      decode.value.readFields(av).map(gen.from)
  }

}
