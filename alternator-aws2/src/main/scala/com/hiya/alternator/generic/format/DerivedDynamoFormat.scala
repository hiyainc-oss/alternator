package com.hiya.alternator.generic.format

import java.util

import com.hiya.alternator.DynamoFormat.Result
import com.hiya.alternator.CompoundDynamoFormat
import shapeless.{LabelledGeneric, Lazy}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

trait DerivedDynamoFormat[T] extends CompoundDynamoFormat[T]

object DerivedDynamoFormat {
  implicit def deriveDecoder[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    decode: Lazy[ReprDynamoFormat[R]]
  ): DerivedDynamoFormat[A] = new DerivedDynamoFormat[A] {

    final override def writeFields(value: A): util.Map[String, AttributeValue] =
      decode.value.writeFields(gen.to(value))

    final override def readFields(av: util.Map[String, AttributeValue]): Result[A] =
      decode.value.readFields(av).map(gen.from)
  }

}
