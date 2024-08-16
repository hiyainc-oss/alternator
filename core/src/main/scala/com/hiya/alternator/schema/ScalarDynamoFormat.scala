package com.hiya.alternator.schema

import com.hiya.alternator.schema.DynamoFormat.Result

import java.nio.ByteBuffer

trait ScalarDynamoFormat[T] extends DynamoFormat[T] {
  def attributeType: ScalarType

  override def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): ScalarDynamoFormat[B] =
    new ScalarDynamoFormat[B] {

      override def attributeType: ScalarType =
        ScalarDynamoFormat.this.attributeType

      override def read[AV: AttributeValue](av: AV): DynamoFormat.Result[B] =
        ScalarDynamoFormat.this.read(av) match {
          case Left(err) => Left(err)
          case Right(value) => f(value)
        }

      override def write[AV: AttributeValue](value: B): AV =
        ScalarDynamoFormat.this.write(g(value))

      override def isEmpty(value: B): Boolean =
        ScalarDynamoFormat.this.isEmpty(g(value))
    }
}

trait StringLikeDynamoFormat[T] extends ScalarDynamoFormat[T] {
  override def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): StringLikeDynamoFormat[B] =
    new StringLikeDynamoFormat[B] {

      override def attributeType: ScalarType =
        StringLikeDynamoFormat.this.attributeType

      override def read[AV: AttributeValue](av: AV): DynamoFormat.Result[B] =
        StringLikeDynamoFormat.this.read(av) match {
          case Left(err) => Left(err)
          case Right(value) => f(value)
        }

      override def write[AV: AttributeValue](value: B): AV =
        StringLikeDynamoFormat.this.write(g(value))

      override def isEmpty(value: B): Boolean =
        false
    }
}

object ScalarDynamoFormat {
  def apply[T](implicit T: ScalarDynamoFormat[T]): ScalarDynamoFormat[T] = T

  def coerceNumeric[T: Numeric](av: String): DynamoFormat.Result[T] =
    Numeric[T]
      .parseString(av)
      .fold[DynamoFormat.Result[T]](Left(DynamoAttributeError.NumberFormatError(av, Numeric[T].toString)))(Right(_))

  trait Instances {
    implicit val stringDynamoValue: StringLikeDynamoFormat[String] = new StringLikeDynamoFormat[String] {

      override def attributeType: ScalarType =
        ScalarType.String

      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[String] =
        if (AV.isNull(av)) Right("")
        else AV.string(av).fold[DynamoFormat.Result[String]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))

      override def write[AV](value: String)(implicit AV: AttributeValue[AV]): AV =
        AV.createString(value)

      override def isEmpty(value: String): Boolean = false
    }

    implicit val byteArray: StringLikeDynamoFormat[Array[Byte]] = new StringLikeDynamoFormat[Array[Byte]] {
      override def attributeType: ScalarType = ScalarType.Binary

      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): Result[Array[Byte]] =
        if (AV.isNull(av)) Right(Array.emptyByteArray)
        else
          AV.byteArray(av).fold[DynamoFormat.Result[Array[Byte]]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))

      override def write[AV](value: Array[Byte])(implicit AV: AttributeValue[AV]): AV =
        AV.createBinary(value)

      override def isEmpty(value: Array[Byte]): Boolean = false
    }

    implicit val byteBuffer: StringLikeDynamoFormat[ByteBuffer] = new StringLikeDynamoFormat[ByteBuffer] {
      override def attributeType: ScalarType = ScalarType.Binary

      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): Result[ByteBuffer] =
        if (AV.isNull(av)) Right(ByteBuffer.wrap(Array.emptyByteArray))
        else
          AV.byteBuffer(av).fold[DynamoFormat.Result[ByteBuffer]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))

      override def write[AV](value: ByteBuffer)(implicit AV: AttributeValue[AV]): AV =
        AV.createBinary(value)

      override def isEmpty(value: ByteBuffer): Boolean = false
    }

    implicit def numberDynamoValue[T: Numeric]: ScalarDynamoFormat[T] = new ScalarDynamoFormat[T] {

      override def attributeType: ScalarType = ScalarType.Numeric

      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[T] =
        AV.numeric(av).fold[DynamoFormat.Result[T]](Left(DynamoAttributeError.AttributeIsNull))(coerceNumeric[T])

      override def write[AV](value: T)(implicit AV: AttributeValue[AV]): AV = AV.createNumeric(value.toString)

      override def isEmpty(value: T): Boolean = false
    }
  }
}
