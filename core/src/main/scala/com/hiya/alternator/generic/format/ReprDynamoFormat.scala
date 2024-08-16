package com.hiya.alternator.generic.format

import cats.instances.either._
import cats.syntax.all._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{AttributeValue, DynamoFormat, RootDynamoFormat}
import shapeless._
import shapeless.labelled.{FieldType, field}

import java.util
import scala.jdk.CollectionConverters._

trait ReprDynamoFormat[T] extends RootDynamoFormat[T]

object ReprDynamoFormat {
  sealed trait HConsBase[T] extends ReprDynamoFormat[T] {
    def writeInternal[AV: AttributeValue](value: T): List[(String, AV)]
    @inline final override def writeFields[AV: AttributeValue](value: T): util.Map[String, AV] =
      writeInternal(value).toMap.asJava
  }

  final class HConsFormat[HK <: Symbol, HV, TKV <: HList](
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[HConsBase[TKV]]
  ) extends HConsBase[FieldType[HK, HV] :: TKV] {
    @inline override def writeInternal[AV: AttributeValue](value: FieldType[HK, HV] :: TKV): List[(String, AV)] = {
      val tail = ct.value.writeInternal(value.tail)
      if (ch.value.isEmpty(value.head)) tail
      else key.value.name -> ch.value.write(value.head) :: tail
    }

    @inline override def readFields[AV](
      av: util.Map[String, AV]
    )(implicit AV: AttributeValue[AV]): DynamoFormat.Result[FieldType[HK, HV] :: TKV] = {
      val head = ch.value
        .read(av.getOrDefault(key.value.name, AV.nullValue))
        .map(field[HK](_))
        .left
        .map(_.withFieldName(key.value.name))
      val tail = ct.value.readFields(av)
      (head, tail).mapN(_ :: _)
    }
  }

  final class HConsFormat0[HK <: Symbol, HV](
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK]
  ) extends HConsBase[FieldType[HK, HV] :: HNil] {
    @inline override def writeInternal[AV](
      value: FieldType[HK, HV] :: HNil
    )(implicit AV: AttributeValue[AV]): List[(String, AV)] = {
      if (!ch.value.isEmpty(value.head)) List(key.value.name -> ch.value.write(value.head))
      else Nil
    }

    @inline override def readFields[AV](
      av: util.Map[String, AV]
    )(implicit AV: AttributeValue[AV]): DynamoFormat.Result[FieldType[HK, HV] :: HNil] = {
      val head = ch.value.read(av.getOrDefault(key.value.name, AV.nullValue)).map(field[HK](_))
      val tail = Right(HNil)
      (head, tail).mapN(_ :: _)
    }
  }

  final implicit def deriveHCons[HK <: Symbol, HV, TKV <: HList](implicit
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[HConsBase[TKV]]
  ): HConsBase[FieldType[HK, HV] :: TKV] = new HConsFormat(ch, key, ct)

  final implicit def deriveHCons0[HK <: Symbol, HV](implicit
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK]
  ): HConsBase[FieldType[HK, HV] :: HNil] = new HConsFormat0(ch, key)

  final class CConsFormat[HK <: Symbol, HV, TKV <: Coproduct](
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ) extends ReprDynamoFormat[FieldType[HK, HV] :+: TKV] {

    @inline override def writeFields[AV: AttributeValue](value: FieldType[HK, HV] :+: TKV): util.Map[String, AV] =
      value.eliminate(
        { value =>
          Map(key.value.name -> ch.value.write(value)).asJava
        },
        ct.value.writeFields[AV]
      )

    @inline override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[FieldType[HK, HV] :+: TKV] =
      Option(av.get(key.value.name)) match {
        case Some(data) => ch.value.read(data).map(v => Inl(field[HK](v)))
        case None => ct.value.readFields(av).map(Inr(_))
      }
  }

  final class CConsFormat0[HK <: Symbol, HV, TKV <: Coproduct](
    ch: Lazy[CaseObjectDynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ) extends ReprDynamoFormat[FieldType[HK, HV] :+: TKV] {
    @inline override def writeFields[AV: AttributeValue](value: FieldType[HK, HV] :+: TKV): util.Map[String, AV] =
      value.eliminate(
        { value =>
          Map(key.value.name -> ch.value.format(key.value.name).write(value)).asJava
        },
        ct.value.writeFields[AV]
      )

    @inline override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[FieldType[HK, HV] :+: TKV] =
      Option(av.get(key.value.name)) match {
        case Some(data) => ch.value.format(key.value.name).read(data).map(v => Inl(field[HK](v)))
        case None => ct.value.readFields(av).map(Inr(_))
      }
  }

  final implicit def deriveCCons[HK <: Symbol, HV, TKV <: Coproduct](implicit
    ch: Lazy[DynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ): ReprDynamoFormat[FieldType[HK, HV] :+: TKV] = new CConsFormat(ch, key, ct)

  final implicit def deriveCConsCaseObject[HK <: Symbol, HV, TKV <: Coproduct](implicit
    ch: Lazy[CaseObjectDynamoFormat[HV]],
    key: Witness.Aux[HK],
    ct: Lazy[ReprDynamoFormat[TKV]]
  ): ReprDynamoFormat[FieldType[HK, HV] :+: TKV] = new CConsFormat0(ch, key, ct)

  final implicit def deriveCNil: ReprDynamoFormat[CNil] = new ReprDynamoFormat[CNil] {
    @inline final override def writeFields[AV: AttributeValue](value: CNil): util.Map[String, AV] = ???
    @inline final override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[CNil] = ???
  }

}
