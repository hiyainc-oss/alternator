package com.hiya.alternator

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, ScalarAttributeType}

import java.nio.ByteBuffer


trait ScalarDynamoFormat[T] extends DynamoFormat[T] {
  def attributeType: ScalarAttributeType

  override def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): ScalarDynamoFormat[B] = new ScalarDynamoFormat[B] {

    override def attributeType: ScalarAttributeType =
      ScalarDynamoFormat.this.attributeType

    override def read(av: AttributeValue): DynamoFormat.Result[B] =
      ScalarDynamoFormat.this.read(av) match {
        case Left(err) => Left(err)
        case Right(value) => f(value)
      }

    override def write(value: B): AttributeValue =
      ScalarDynamoFormat.this.write(g(value))

    override def isEmpty(value: B): Boolean =
      ScalarDynamoFormat.this.isEmpty(g(value))
  }
}

trait StringLikeDynamoFormat[T] extends ScalarDynamoFormat[T] {
  override def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): StringLikeDynamoFormat[B] = new StringLikeDynamoFormat[B] {

    override def attributeType: ScalarAttributeType =
      StringLikeDynamoFormat.this.attributeType

    override def read(av: AttributeValue): DynamoFormat.Result[B] =
      StringLikeDynamoFormat.this.read(av) match {
        case Left(err) => Left(err)
        case Right(value) => f(value)
      }

    override def write(value: B): AttributeValue =
      StringLikeDynamoFormat.this.write(g(value))

    override def isEmpty(value: B): Boolean =
      StringLikeDynamoFormat.this.isEmpty(g(value))
  }
}

object ScalarDynamoFormat {
  def apply[T](implicit T: ScalarDynamoFormat[T]): ScalarDynamoFormat[T] = T

  def coerceNumeric[T: Numeric](av: String): DynamoFormat.Result[T] =
    Numeric[T].parseString(av).fold[DynamoFormat.Result[T]](Left(DynamoAttributeError.NumberFormatError(av, Numeric[T].toString)))(Right(_))

  trait Instances {
    implicit val stringDynamoValue: StringLikeDynamoFormat[String] = new StringLikeDynamoFormat[String] {

      override def attributeType: ScalarAttributeType =
        ScalarAttributeType.S

      override def read(av: AttributeValue): DynamoFormat.Result[String] =
        if(av.nul()) Right("")
        else Option(av.s()).fold[DynamoFormat.Result[String]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))

      override def write(value: String): AttributeValue = AttributeValue.builder().s(value).build()

      override def isEmpty(value: String): Boolean = value.isEmpty
    }

    implicit val binaryDynamoValue: StringLikeDynamoFormat[SdkBytes] = new StringLikeDynamoFormat[SdkBytes] {

      override def attributeType: ScalarAttributeType =
        ScalarAttributeType.B

      override def read(av: AttributeValue): DynamoFormat.Result[SdkBytes] = {
        if(av.nul()) Right(SdkBytes.fromByteArray(Array.emptyByteArray))
        else Option(av.b()).fold[DynamoFormat.Result[SdkBytes]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))
      }

      override def write(value: SdkBytes): AttributeValue = AttributeValue.builder().b(value).build()

      override def isEmpty(value: SdkBytes): Boolean = false
    }

    implicit val byteArray: StringLikeDynamoFormat[Array[Byte]] = binaryDynamoValue.emap({ x =>
      Right(x.asByteArray())
    },
      x => SdkBytes.fromByteArray(x)
    )

    implicit val byteString: StringLikeDynamoFormat[ByteBuffer] =
      binaryDynamoValue.emap({x =>
        Right(ByteBuffer.wrap(x.asByteArray()))
      }, x => SdkBytes.fromByteArray(x.array()))

    implicit def numberDynamoValue[T: Numeric]: ScalarDynamoFormat[T] = new ScalarDynamoFormat[T] {

      override def attributeType: ScalarAttributeType =
        ScalarAttributeType.N

      override def read(av: AttributeValue): DynamoFormat.Result[T] =
        Option(av.n()).fold[DynamoFormat.Result[T]](Left(DynamoAttributeError.AttributeIsNull))(coerceNumeric[T])

      override def write(value: T): AttributeValue = AttributeValue.builder().n(value.toString).build()

      override def isEmpty(value: T): Boolean = false
    }
  }
}
