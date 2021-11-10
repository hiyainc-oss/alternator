package com.hiya.alternator

import akka.util.ByteString
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScalarAttributeType}

import java.nio.ByteBuffer
import scala.reflect.ClassTag

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

object ScalarDynamoFormat {
  def coerceNumeric[T: Numeric](av: String): DynamoFormat.Result[T] =
    Numeric[T].parseString(av).fold[DynamoFormat.Result[T]](Left(DynamoAttributeError.NumberFormatError(av, Numeric[T].toString)))(Right(_))

  trait Instances {
    implicit val stringDynamoValue: ScalarDynamoFormat[String] = new ScalarDynamoFormat[String] {

      override def attributeType: ScalarAttributeType =
        ScalarAttributeType.S

      override def read(av: AttributeValue): DynamoFormat.Result[String] =
        if(av.getNULL()) Right("")
        else Option(av.getS).fold[DynamoFormat.Result[String]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))

      override def write(value: String): AttributeValue = new AttributeValue().withS(value)

      override def isEmpty(value: String): Boolean = value.isEmpty
    }

    implicit val binaryDynamoValue: ScalarDynamoFormat[ByteBuffer] = new ScalarDynamoFormat[ByteBuffer] {

      override def attributeType: ScalarAttributeType =
        ScalarAttributeType.B

      override def read(av: AttributeValue): DynamoFormat.Result[ByteBuffer] = {
        if(av.getNULL()) Right(ByteBuffer.wrap(Array.emptyByteArray))
        else Option(av.getB).fold[DynamoFormat.Result[ByteBuffer]](Left(DynamoAttributeError.AttributeIsNull))(Right(_))
      }

      override def write(value: ByteBuffer): AttributeValue = new AttributeValue().withB(value)

      override def isEmpty(value: ByteBuffer): Boolean = false
    }

    implicit val byteArray: ScalarDynamoFormat[Array[Byte]] = binaryDynamoValue.emap({ x =>
      Right(x.array())
    },
      x => ByteBuffer.wrap(x)
    )
    implicit val byteString: ScalarDynamoFormat[ByteString] =
      binaryDynamoValue.emap({x =>
        Right(ByteString(x))
      }, x => x.toByteBuffer)

    implicit def numberDynamoValue[T: Numeric : ClassTag]: ScalarDynamoFormat[T] = new ScalarDynamoFormat[T] {

      override def attributeType: ScalarAttributeType =
        ScalarAttributeType.N

      override def read(av: AttributeValue): DynamoFormat.Result[T] =
        Option(av.getN).fold[DynamoFormat.Result[T]](Left(DynamoAttributeError.AttributeIsNull))(coerceNumeric[T])

      override def write(value: T): AttributeValue = new AttributeValue().withN(value.toString)

      override def isEmpty(value: T): Boolean = false
    }
  }
}
