package com.hiya.alternator

import cats.instances.either._
import cats.instances.list._
import cats.syntax.all._
import com.hiya.alternator.DynamoFormatInstances.SetDynamoFormat
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._
import scala.collection.compat._


trait DynamoFormatInstances {


  implicit val booleanDynamoFormat: DynamoFormat[Boolean] = new DynamoFormat[Boolean] {
    override def read(av: AttributeValue): DynamoFormat.Result[Boolean] = Right(Boolean.unbox(av.bool()))
    override def write(value: Boolean): AttributeValue = if (value) DynamoFormat.TrueAttributeValue else DynamoFormat.FalseAttributeValue
    override def isEmpty(value: Boolean): Boolean = false
  }

  implicit val stringSetDynamoFormat: DynamoFormat[Set[String]] =
    new SetDynamoFormat[String](
      _.ss().asScala.asRight,
      x => AttributeValue.builder().ss(x.asJava).build()
    )

  implicit def numberSetDynamoFormat[T: Numeric]: DynamoFormat[Set[T]] =
    new SetDynamoFormat[T](
      _.ns().asScala.toList.traverse(ScalarDynamoFormat.coerceNumeric[T]),
      x => AttributeValue.builder().ns(x.map(_.toString).asJava).build()
    )

  implicit val byteSetDynamoFormat: DynamoFormat[Set[SdkBytes]] =
    new SetDynamoFormat[SdkBytes](
      _.bs().asScala.asRight,
      x => AttributeValue.builder().bs(x.asJava).build()
    )

  implicit val byteArraySetDynamoFormat: DynamoFormat[Set[Array[Byte]]] =
    new SetDynamoFormat[Array[Byte]](
      _.bs().asScala.map(_.asByteArray()).asRight,
      x => AttributeValue.builder().bs(x.map(SdkBytes.fromByteArray).asJava).build()
    )

  implicit val byteStringSetDynamoFormat: DynamoFormat[Set[ByteBuffer]] = {
    byteArraySetDynamoFormat.emap[Set[ByteBuffer]](
      {x => Right(x.map(ByteBuffer.wrap))}, {x => x.map(_.array())}
    )
  }
}

object DynamoFormatInstances {

  class SetDynamoFormat[T](
    reader: AttributeValue => DynamoFormat.Result[IterableOnce[T]],
    writer: Set[T] => AttributeValue
  ) extends DynamoFormat[Set[T]] {
    override def read(av: AttributeValue): DynamoFormat.Result[Set[T]] = {
      if (av.nul()) Set.empty.asRight
      else reader(av).map(Set.from)
    }

    override def write(value: Set[T]): AttributeValue =
      if (isEmpty(value)) DynamoFormat.NullAttributeValue
      else writer(value)

    override def isEmpty(value: Set[T]): Boolean =
      value.isEmpty

  }
}
