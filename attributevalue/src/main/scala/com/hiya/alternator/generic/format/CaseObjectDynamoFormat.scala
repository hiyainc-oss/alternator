package com.hiya.alternator.generic.format

import com.hiya.alternator.DynamoAttributeError.IllegalDistriminator
import com.hiya.alternator.DynamoFormat
import shapeless.{HNil, LabelledGeneric}

trait CaseObjectDynamoFormat[T] {
  def format(name: String): DynamoFormat[T]
}

object CaseObjectDynamoFormat {
  final class Instance[A](gen: LabelledGeneric.Aux[A, HNil]) extends CaseObjectDynamoFormat[A] {
    override def format(name: String): DynamoFormat[A] = {
      DynamoFormat.stringDynamoValue.emap[A](x => {
        if (name == x) Right(gen.from(HNil)) else Left(IllegalDistriminator)
      }, { _ =>
        name
      })
    }
  }

  final implicit def deriveDecoder[A](implicit gen: LabelledGeneric.Aux[A, HNil]): CaseObjectDynamoFormat[A] =
    new Instance(gen)
}
