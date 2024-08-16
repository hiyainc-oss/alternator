package com.hiya.alternator.schema

import cats.instances.either._
import cats.syntax.all._
import com.hiya.alternator.schema.DynamoFormat.Result

import java.nio.ByteBuffer
import scala.collection.compat._
import scala.jdk.CollectionConverters._

trait DynamoFormatInstances {

  implicit val booleanDynamoFormat: DynamoFormat[Boolean] = new DynamoFormat[Boolean] {
    override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[Boolean] =
      AV.bool(av) match {
        case Some(value) => value.asRight
        case None => DynamoAttributeError.AttributeIsNull.asLeft
      }

    override def write[AV](value: Boolean)(implicit AV: AttributeValue[AV]): AV =
      if (value) AV.trueValue else AV.falseValue

    override def isEmpty(value: Boolean): Boolean = false
  }

  implicit val stringSetDynamoFormat: DynamoFormat[Set[String]] =
    new DynamoFormatInstances.SetDynamoFormat[String] {
      override protected def reader[AV](
        av: AV
      )(implicit AV: AttributeValue[AV]): DynamoFormat.Result[IterableOnce[String]] =
        AV.stringSet(av) match {
          case Some(value) => value.asScala.asRight
          case None => DynamoAttributeError.TypeError(av, "SS").asLeft
        }

      override protected def writer[AV](value: Set[String])(implicit AV: AttributeValue[AV]): AV =
        AV.createStringSet(value.asJava)
    }

  implicit def numberSetDynamoFormat[T: Numeric]: DynamoFormatInstances.SetDynamoFormat[T] =
    new DynamoFormatInstances.SetDynamoFormat[T] {
      override protected def reader[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[IterableOnce[T]] =
        AV.numberSet(av) match {
          case Some(value) => value.asScala.toList.traverse(ScalarDynamoFormat.coerceNumeric[T])
          case None => DynamoAttributeError.TypeError(av, "NS").asLeft
        }

      override protected def writer[AV](value: Set[T])(implicit AV: AttributeValue[AV]): AV =
        AV.createNumberSet(value.map(_.toString).asJava)
    }

  implicit val byteBufferSetDynamoFormat: DynamoFormat[Set[ByteBuffer]] =
    new DynamoFormatInstances.SetDynamoFormat[ByteBuffer] {
      override protected def reader[AV](av: AV)(implicit AV: AttributeValue[AV]): Result[IterableOnce[ByteBuffer]] =
        AV.byteBufferSet(av) match {
          case Some(value) => value.asScala.asRight
          case None => DynamoAttributeError.TypeError(av, "BS").asLeft
        }

      override protected def writer[AV](value: Set[ByteBuffer])(implicit AV: AttributeValue[AV]): AV = {
        AV.createByteBufferSet(value.asJava)
      }
    }
}

object DynamoFormatInstances {

  abstract class SetDynamoFormat[T] extends DynamoFormat[Set[T]] {
    protected def reader[AV: AttributeValue](av: AV): DynamoFormat.Result[IterableOnce[T]]
    protected def writer[AV: AttributeValue](value: Set[T]): AV

    override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[Set[T]] = {
      if (AV.isNull(av)) Set.empty[T].asRight
      else reader(av).map(Set.from)
    }

    override def write[AV](value: Set[T])(implicit AV: AttributeValue[AV]): AV =
      if (isEmpty(value)) AV.nullValue
      else writer(value)

    override def isEmpty(value: Set[T]): Boolean =
      value.isEmpty

  }
}
