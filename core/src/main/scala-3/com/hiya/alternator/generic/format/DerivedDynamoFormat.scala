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
      case s: Mirror.SumOf[A] => deriveSum(using s)

  // ------------------------------------------------------------------
  // Products (case class)
  // ------------------------------------------------------------------

  private inline def deriveProduct[A](using m: Mirror.ProductOf[A]): DerivedDynamoFormat[A] =
    val labels = constValueTuple[m.MirroredElemLabels].productIterator.toList.asInstanceOf[List[String]]
    val formats = summonAll[Tuple.Map[m.MirroredElemTypes, DynamoFormat]].toList
      .asInstanceOf[List[DynamoFormat[Any]]]

    new DerivedDynamoFormat[A]:
      override def writeFields[AV: AttributeValue](value: A): util.Map[String, AV] =
        val product = value.asInstanceOf[Product]
        val entries = labels
          .zip(formats)
          .zip(product.productIterator.toList)
          .flatMap { case ((label, fmt), v) =>
            if fmt.isEmpty(v) then None
            else Some(label -> fmt.write[AV](v))
          }
        entries.toMap.asJava

      override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[A] =
        val AV = summon[AttributeValue[AV]]
        labels
          .zip(formats)
          .traverse { case (label, fmt) =>
            fmt.read(av.getOrDefault(label, AV.nullValue)).left.map(_.withFieldName(label))
          }
          .map(values => m.fromProduct(Tuple.fromArray(values.toArray)))

  // ------------------------------------------------------------------
  // Sums (sealed trait)
  // ------------------------------------------------------------------

  /** Per-variant codec: knows how to read/write its value under a discriminator key. Not private — inline methods
    * expand at call sites outside this object.
    */
  sealed trait VariantDef[A]:
    def write[AV: AttributeValue](label: String, value: A): AV
    def read[AV: AttributeValue](label: String, av: AV): Result[A]

  /** Case object variant: writes `{ label -> S(label) }`, reads the singleton back. */
  final class CaseObjectDef[A](singleton: A) extends VariantDef[A]:
    def write[AV: AttributeValue](label: String, value: A): AV =
      summon[AttributeValue[AV]].createString(label)
    def read[AV: AttributeValue](label: String, av: AV): Result[A] =
      summon[AttributeValue[AV]].string(av) match
        case Some(`label`) => Right(singleton)
        case _ => Left(DynamoAttributeError.IllegalDistriminator)

  /** Case class variant: delegates to `RootDynamoFormat` for the variant type. */
  final class CaseClassDef[A](fmt: RootDynamoFormat[A]) extends VariantDef[A]:
    def write[AV: AttributeValue](label: String, value: A): AV = fmt.write[AV](value)
    def read[AV: AttributeValue](label: String, av: AV): Result[A] = fmt.read[AV](av)

  private inline def summonVariants[T <: Tuple]: List[VariantDef[Any]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (h *: t) =>
        summonVariant[h].asInstanceOf[VariantDef[Any]] :: summonVariants[t]

  /** Determine if variant A is a case object (singleton) or case class.
    *
    * Priority:
    *  1. Case object (empty Mirror product) — detected structurally so it is never mistaken for a
    *     case class even when an auto-derived `RootDynamoFormat` is in scope.
    *  2. Explicit user-provided `RootDynamoFormat[A]` — wins over structural derivation, mirroring
    *     the Scala 2 behaviour where `deriveCCons` summons `Lazy[DynamoFormat[HV]]` so the user's
    *     implicit takes precedence.
    *  3. Structural product derivation — fallback when no user format exists.
    *
    * The check is for each *variant* type `A`, never for the enclosing sum type `S` itself, so
    * there is no risk of infinite recursion.
    *
    * IMPORTANT: The case-object arm (1) must precede the `RootDynamoFormat` arm (2). With
    * `auto._` in scope every type that has a `Mirror` automatically has a `RootDynamoFormat`, so
    * checking the format first would wrongly treat case objects as case classes.
    */
  private inline def summonVariant[A]: VariantDef[A] =
    summonFrom {
      // (1) Case object: a singleton with no fields — detect via Mirror before checking any format.
      case m: Mirror.ProductOf[A] =>
        inline erasedValue[m.MirroredElemTypes] match
          case _: EmptyTuple =>
            new CaseObjectDef[A](m.fromProduct(EmptyTuple))
          case _ =>
            // (2) Case class: prefer an explicit user-provided format over structural derivation.
            summonFrom {
              case f: RootDynamoFormat[A] => new CaseClassDef[A](f)
              case mp: Mirror.ProductOf[A] => new CaseClassDef[A](deriveProduct(using mp))
            }
    }

  private inline def deriveSum[A](using m: Mirror.SumOf[A]): DerivedDynamoFormat[A] =
    val labels = constValueTuple[m.MirroredElemLabels].productIterator.toList.asInstanceOf[List[String]]
    val variants = summonVariants[m.MirroredElemTypes].asInstanceOf[List[VariantDef[A]]]

    new DerivedDynamoFormat[A]:
      override def writeFields[AV: AttributeValue](value: A): util.Map[String, AV] =
        val ord = m.ordinal(value)
        val label = labels(ord)
        Map(label -> variants(ord).write[AV](label, value)).asJava

      override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[A] =
        labels
          .zip(variants)
          .iterator
          .collectFirst {
            case (label, variant) if av.containsKey(label) =>
              variant.read[AV](label, av.get(label))
          }
          .getOrElse(Left(DynamoAttributeError.IllegalDistriminator))
