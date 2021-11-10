package com.hiya.alternator

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import cats.syntax.traverse._
import cats.instances.list._
import cats.instances.either._
import com.hiya.alternator.generic.util.Exported

import scala.jdk.CollectionConverters._


trait DynamoFormat[T] extends Serializable {
  def read(av: AttributeValue): DynamoFormat.Result[T]
  def write(value: T): AttributeValue
  final def writeIfNotEmpty(value: T): Option[AttributeValue] = {
    if (isEmpty(value)) None
    else Some(write(value))
  }
  def isEmpty(value: T): Boolean

  def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): DynamoFormat[B] = new DynamoFormat[B] {
    override def read(av: AttributeValue): DynamoFormat.Result[B] =
      DynamoFormat.this.read(av) match {
        case Left(err) => Left(err)
        case Right(value) => f(value)
      }

    override def write(value: B): AttributeValue =
      DynamoFormat.this.write(g(value))

    override def isEmpty(value: B): Boolean =
      DynamoFormat.this.isEmpty(g(value))
  }
}

object DynamoFormat extends DynamoFormatInstances with ScalarDynamoFormat.Instances with CompoundDynamoFormat.Instances with LowPriorityDynamoFormats {
  final def apply[T](implicit T: DynamoFormat[T]): DynamoFormat[T] = T

  final type Result[T] = Either[DynamoAttributeError, T]

  val NullAttributeValue: AttributeValue = new AttributeValue().withNULL(true)
  val TrueAttributeValue: AttributeValue = new AttributeValue().withBOOL(true)
  val FalseAttributeValue: AttributeValue = new AttributeValue().withBOOL(false)
  val EmptyListAttributeValue: AttributeValue = new AttributeValue().withL(List.empty[AttributeValue].asJava)
  val EmptyMapAttributeValue: AttributeValue = new AttributeValue().withM(Map.empty[String, AttributeValue].asJava)

  implicit def optionDynamoFormat[T: DynamoFormat]: DynamoFormat[Option[T]] = new DynamoFormat[Option[T]] {

    override def read(av: AttributeValue): DynamoFormat.Result[Option[T]] =
      if (av.getNULL) Right(None)
      else DynamoFormat[T].read(av).map(Some(_))

    override def write(value: Option[T]): AttributeValue =
      value.fold(DynamoFormat.NullAttributeValue)(DynamoFormat[T].write)

    override def isEmpty(value: Option[T]): Boolean = value.fold(true)(DynamoFormat[T].isEmpty)
  }

  implicit def listDynamoFormat[T: DynamoFormat]: DynamoFormat[List[T]] = new DynamoFormat[List[T]] {
    override def read(av: AttributeValue): DynamoFormat.Result[List[T]] = {
      if(av.getL != null) av.getL.asScala.toList.traverse(DynamoFormat[T].read)
      else Left(DynamoAttributeError.AttributeIsNull)
    }

    override def write(value: List[T]): AttributeValue =
      if (value.isEmpty) DynamoFormat.EmptyListAttributeValue
      else new AttributeValue().withL(value.map(x => DynamoFormat[T].writeIfNotEmpty(x).getOrElse(DynamoFormat.NullAttributeValue)).asJava)

    override def isEmpty(value: List[T]): Boolean = false
  }


}

private [alternator] trait LowPriorityDynamoFormats {

  final implicit def importedDynamoFormat[A](implicit exported: Exported[CompoundDynamoFormat[A]]): CompoundDynamoFormat[A] = exported.instance
}


