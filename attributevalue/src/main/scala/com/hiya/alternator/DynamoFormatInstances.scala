package com.hiya.alternator

import cats.instances.either._
import cats.instances.list._
import cats.syntax.all._
import DynamoFormatInstances.SetDynamoFormat
import com.amazonaws.services.dynamodbv2.model.AttributeValue

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

trait DynamoFormatInstances {


  implicit val booleanDynamoFormat: DynamoFormat[Boolean] = new DynamoFormat[Boolean] {
    override def read(av: AttributeValue): DynamoFormat.Result[Boolean] = Right(Boolean.unbox(av.getBOOL))
    override def write(value: Boolean): AttributeValue = if (value) DynamoFormat.TrueAttributeValue else DynamoFormat.FalseAttributeValue
    override def isEmpty(value: Boolean): Boolean = false
  }

  implicit val stringSetDynamoFormat: DynamoFormat[Set[String]] =
    new SetDynamoFormat[String](
      _.getSS.asScala.asRight,
      x => if (x.isEmpty) DynamoFormat.NullAttributeValue
           else new AttributeValue().withSS(x.asJava)
    )

  implicit def numberSetDynamoFormat[T: Numeric]: DynamoFormat[Set[T]] =
    new SetDynamoFormat[T](
      _.getNS.asScala.toList.traverse(ScalarDynamoFormat.coerceNumeric[T]),
      x => if(x.isEmpty) DynamoFormat.NullAttributeValue
           else new AttributeValue().withNS(x.map(_.toString).asJava)
    )

  implicit val byteSetDynamoFormat: DynamoFormat[Set[ByteBuffer]] =
    new SetDynamoFormat[ByteBuffer](
      _.getBS.asScala.asRight,
      x => new AttributeValue().withBS(x.asJava)
    )

  implicit val byteArraySetDynamoFormat: DynamoFormat[Set[Array[Byte]]] =
    new SetDynamoFormat[Array[Byte]](
      _.getBS.asScala.map(_.array()).asRight,
      x => new AttributeValue().withBS(x.map(ByteBuffer.wrap).asJava)
    )
}

object DynamoFormatInstances {

  class SetDynamoFormat[T](
    reader: AttributeValue => DynamoFormat.Result[IterableOnce[T]],
    writer: Set[T] => AttributeValue
  ) extends DynamoFormat[Set[T]] {
    override def read(av: AttributeValue): DynamoFormat.Result[Set[T]] = {
      if (av.getNULL) Set.empty.asRight
      else reader(av).map(Set.from)
    }

    override def write(value: Set[T]): AttributeValue =
      writer(value)

    override def isEmpty(value: Set[T]): Boolean =
      value.isEmpty

  }
}
