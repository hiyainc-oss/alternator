package com.hiya.alternator

import com.amazonaws.services.dynamodbv2.model.AttributeValue

sealed trait DynamoAttributeError {
  def message: String
  def withFieldName(name: String): DynamoAttributeError.FieldFormatError
}

object DynamoAttributeError {
  final case class FieldFormatError(fieldName: String, error: FormatError) extends DynamoAttributeError {
    override def message: String = s"Error in $fieldName: ${error.message}"
    override def withFieldName(name: String): FieldFormatError = FieldFormatError(s"$name.$fieldName", error)
  }

  sealed trait FormatError extends DynamoAttributeError {
    override def withFieldName(name: String): FieldFormatError = FieldFormatError(name, this)
  }

  final case class NumberFormatError(original: String, typeName: String) extends FormatError {
    override def message: String = s"String $original cannot be parsed as $typeName"
  }

  final case class TypeError(av: AttributeValue, typeName: String) extends FormatError {
    override def message: String = s"AttributeValue is not a $typeName: $av"
  }

  final case object AttributeIsNull extends FormatError {
    override def message: String = "should not be null"
  }

  final case object IllegalDistriminator extends FormatError {
    override def message: String = "Cannot find valid discriminator"
  }
}
