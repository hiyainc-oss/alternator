package com.hiya.alternator.generic.format

import java.util

import cats.instances.either._
import cats.syntax.all._
import com.hiya.alternator.DynamoFormat.Result
import com.hiya.alternator.{CompoundDynamoFormat, DynamoFormat}
import shapeless._
import shapeless.labelled.{FieldType, field}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._


trait ReprDynamoFormat[T] extends CompoundDynamoFormat[T]

object ReprDynamoFormat {
  sealed trait HConsBase[T] extends ReprDynamoFormat[T] {
    def writeInternal(value: T): List[(String, AttributeValue)]
    @inline final override def writeFields(value: T): util.Map[String, AttributeValue] =
      writeInternal(value).toMap.asJava
  }

  final class HConsFormat[HK <: Symbol, HV, TKV <: HList](
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[HConsBase[TKV]]
  ) extends HConsBase[FieldType[HK, HV] ::  TKV] {
    @inline override def writeInternal(value: FieldType[HK, HV] :: TKV): List[(String, AttributeValue)] = {
      val tail = ct.value.writeInternal(value.tail)
      if (ch.value.isEmpty(value.head)) tail
      else key.value.name -> ch.value.write(value.head) :: tail
    }

    @inline override def readFields(av: util.Map[String, AttributeValue]): DynamoFormat.Result[FieldType[HK, HV] :: TKV] = {
      val head = ch.value.read(av.getOrDefault(key.value.name, DynamoFormat.NullAttributeValue)).map(field[HK](_))
        .left.map(_.withFieldName(key.value.name))
      val tail = ct.value.readFields(av)
      (head, tail).mapN(_ :: _)
    }
  }

  final class HConsFormat0[HK <: Symbol, HV](
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK]
  ) extends HConsBase[FieldType[HK, HV] :: HNil] {
    @inline override def writeInternal(value: FieldType[HK, HV] :: HNil): List[(String, AttributeValue)] = {
      if (!ch.value.isEmpty(value.head)) List(key.value.name -> ch.value.write(value.head))
      else Nil
    }

    @inline override def readFields(av: util.Map[String, AttributeValue]): DynamoFormat.Result[FieldType[HK, HV] :: HNil] = {
      val head = ch.value.read(av.getOrDefault(key.value.name, DynamoFormat.NullAttributeValue)).map(field[HK](_))
      val tail = Right(HNil)
      (head, tail).mapN(_ :: _)
    }
  }

  final implicit def deriveHCons[HK <: Symbol, HV, TKV <: HList](
    implicit
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[HConsBase[TKV]]
  ): HConsBase[FieldType[HK, HV] :: TKV] = new HConsFormat(ch, key, ct)

  final implicit def deriveHCons0[HK <: Symbol, HV](
    implicit
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK]
  ): HConsBase[FieldType[HK, HV] :: HNil] = new HConsFormat0(ch, key)

  final class CConsFormat[HK <: Symbol, HV, TKV <: Coproduct](
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ) extends ReprDynamoFormat[FieldType[HK, HV] :+: TKV] {

    @inline override def writeFields(value: FieldType[HK, HV] :+: TKV): util.Map[String, AttributeValue] =
      value.eliminate({ value =>
        Map(key.value.name -> ch.value.write(value)).asJava
      }, ct.value.writeFields)

    @inline override def readFields(av: util.Map[String, AttributeValue]): Result[FieldType[HK, HV] :+: TKV] =
      Option(av.get(key.value.name)) match {
        case Some(data) => ch.value.read(data).map(v => Inl(field[HK](v)))
        case None       => ct.value.readFields(av).map(Inr(_))
      }
  }

  final class CConsFormat0[HK <: Symbol, HV, TKV <: Coproduct](
    ch: Lazy[CaseObjectDynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ) extends ReprDynamoFormat[FieldType[HK, HV] :+: TKV] {
    @inline override def writeFields(value: FieldType[HK, HV] :+: TKV): util.Map[String, AttributeValue] =
      value.eliminate({ value =>
        Map(key.value.name -> ch.value.format(key.value.name).write(value)).asJava
      }, ct.value.writeFields)

    @inline override def readFields(av: util.Map[String, AttributeValue]): Result[FieldType[HK, HV] :+: TKV] =
      Option(av.get(key.value.name)) match {
        case Some(data) => ch.value.format(key.value.name).read(data).map(v => Inl(field[HK](v)))
        case None       => ct.value.readFields(av).map(Inr(_))
      }
  }

  final implicit def deriveCCons[HK <: Symbol, HV, TKV <: Coproduct](
    implicit
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ): ReprDynamoFormat[FieldType[HK, HV] :+: TKV] = new CConsFormat(ch, key, ct)

  final implicit def deriveCConsCaseObject[HK <: Symbol, HV, TKV <: Coproduct](
    implicit
    ch: Lazy[CaseObjectDynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ): ReprDynamoFormat[FieldType[HK, HV] :+: TKV] = new CConsFormat0(ch, key, ct)

  final implicit def deriveCNil: ReprDynamoFormat[CNil] = new ReprDynamoFormat[CNil] {
    @inline final override def writeFields(value: CNil): util.Map[String, AttributeValue] = ???
    @inline final override def readFields(av: util.Map[String, AttributeValue]): Result[CNil] = ???
  }

}
