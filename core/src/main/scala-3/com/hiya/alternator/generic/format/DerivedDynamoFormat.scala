package com.hiya.alternator.generic.format

import cats.instances.either.*
import cats.instances.list.*
import cats.syntax.traverse.*
import com.hiya.alternator.schema.{AttributeValue, DynamoAttributeError, DynamoFormat, RootDynamoFormat}
import com.hiya.alternator.schema.DynamoFormat.Result

import java.util
import scala.compiletime.*
import scala.deriving.*
import scala.jdk.CollectionConverters.*

trait DerivedDynamoFormat[T] extends RootDynamoFormat[T]

object DerivedDynamoFormat:

  inline def derive[A](using m: Mirror.Of[A]): DerivedDynamoFormat[A] =
    inline m match
      case p: Mirror.ProductOf[A] => deriveProduct(using p)
      case s: Mirror.SumOf[A]     => deriveSum(using s)

  // ------------------------------------------------------------------
  // Products (case class)
  // ------------------------------------------------------------------

  private inline def deriveProduct[A](using m: Mirror.ProductOf[A]): DerivedDynamoFormat[A] =
    val labels  = constValueTuple[m.MirroredElemLabels].productIterator.toList.asInstanceOf[List[String]]
    val formats = summonAll[Tuple.Map[m.MirroredElemTypes, DynamoFormat]].toList
      .asInstanceOf[List[DynamoFormat[Any]]]

    new DerivedDynamoFormat[A]:
      override def writeFields[AV: AttributeValue](value: A): util.Map[String, AV] =
        val product = value.asInstanceOf[Product]
        val entries = labels.zip(formats).zip(product.productIterator.toList)
          .flatMap { case ((label, fmt), v) =>
            if fmt.isEmpty(v) then None
            else Some(label -> fmt.write[AV](v))
          }
        entries.toMap.asJava

      override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[A] =
        val AV = summon[AttributeValue[AV]]
        labels.zip(formats).traverse { case (label, fmt) =>
          fmt.read(av.getOrDefault(label, AV.nullValue))
            .left.map(_.withFieldName(label))
        }.map(values => m.fromProduct(Tuple.fromArray(values.toArray)))

  // ------------------------------------------------------------------
  // Sums — placeholder, implemented in Task 5
  // ------------------------------------------------------------------

  private inline def deriveSum[A](using m: Mirror.SumOf[A]): DerivedDynamoFormat[A] =
    throw new NotImplementedError("Sum derivation not yet implemented — see Task 5")
