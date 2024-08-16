package com.hiya.alternator.schema

import cats.implicits.toTraverseOps
import com.hiya.alternator.generic.util.Exported

import scala.jdk.CollectionConverters._

trait DynamoFormat[T] extends Serializable {
  def read[AV: AttributeValue](av: AV): DynamoFormat.Result[T]
  def write[AV: AttributeValue](value: T): AV
  def isEmpty(value: T): Boolean

  def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): DynamoFormat[B] =
    new DynamoFormat[B] {

      override def read[AV: AttributeValue](av: AV): DynamoFormat.Result[B] =
        DynamoFormat.this.read(av) match {
          case Left(err) => Left(err)
          case Right(value) => f(value)
        }

      override def write[AV: AttributeValue](value: B): AV =
        DynamoFormat.this.write(g(value))

      override def isEmpty(value: B): Boolean =
        DynamoFormat.this.isEmpty(g(value))
    }
}

object DynamoFormat
  extends DynamoFormatInstances
  with ScalarDynamoFormat.Instances
  with RootDynamoFormat.Instances
  with LowPriorityDynamoFormats {
  final def apply[T](implicit T: DynamoFormat[T]): DynamoFormat[T] = T

  final type Result[T] = Either[DynamoAttributeError, T]

  implicit def optionDynamoFormat[T: DynamoFormat]: DynamoFormat[Option[T]] =
    new DynamoFormat[Option[T]] {
      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): Result[Option[T]] =
        if (AV.isNull(av)) Right(None)
        else DynamoFormat[T].read(av).map(Some(_))

      override def write[AV](value: Option[T])(implicit AV: AttributeValue[AV]): AV =
        value.fold(AV.nullValue)(DynamoFormat[T].write[AV])

      override def isEmpty(value: Option[T]): Boolean = value.isEmpty
    }

  implicit def listDynamoFormat[T: DynamoFormat]: DynamoFormat[List[T]] =
    new DynamoFormat[List[T]] {

      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): Result[List[T]] =
        AV.list(av) match {
          case Some(l) => l.asScala.toList.traverse(DynamoFormat[T].read[AV])
          case None => Left(DynamoAttributeError.AttributeIsNull)
        }

      override def write[AV](value: List[T])(implicit AV: AttributeValue[AV]): AV =
        if (value.isEmpty) AV.emptyList
        else AV.createList(value.map(x => DynamoFormat[T].write(x)).asJava)

      override def isEmpty(value: List[T]): Boolean = false
    }

}

private[alternator] trait LowPriorityDynamoFormats {

  final implicit def importedDynamoFormat[A](implicit
    exported: Exported[RootDynamoFormat[A]]
  ): RootDynamoFormat[A] = exported.instance
}
