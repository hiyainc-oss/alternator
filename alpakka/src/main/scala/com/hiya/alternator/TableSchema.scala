package com.hiya.alternator

import java.util

import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, ScalarAttributeType}

import scala.jdk.CollectionConverters._
import cats.syntax.apply._
import cats.instances.either._


abstract class TableSchema[V](val serializeValue: CompoundDynamoFormat[V]) {
  type PKType

  def serializePK(pk: PKType): util.Map[String, AttributeValue]
  def extract(av: util.Map[String, AttributeValue]): DynamoFormat.Result[PKType]
  def schema: List[(String, ScalarAttributeType)]
}

object TableSchema {
  type Aux[V, PK] = TableSchema[V] { type PKType = PK }

  def schemaWithPK[PK, V](pkField: String)(
    implicit PK: ScalarDynamoFormat[PK],
    V: CompoundDynamoFormat[V]
  ): TableSchema.Aux[V, PK] = new TableSchema[V](V) {
    override type PKType = PK

    override def serializePK(pk: PKType): util.Map[String, AttributeValue] =
      Map(pkField -> PK.write(pk)).asJava

    override def extract(av: util.Map[String, AttributeValue]): DynamoFormat.Result[PKType] = {
      Option(av.get(pkField)).fold[DynamoFormat.Result[PKType]](Left(DynamoAttributeError.AttributeIsNull))(PK.read)
    }

    override def schema: List[(String, ScalarAttributeType)] = pkField -> PK.attributeType :: Nil
  }

  def schemaWithRK[PK, RK, V](pkField: String, rkField: String)(
    implicit
    V: CompoundDynamoFormat[V],
    PK: ScalarDynamoFormat[PK],
    RK: ScalarDynamoFormat[RK]
  ): TableSchema.Aux[V, (PK, RK)] = new TableSchema[V](V) {
    override type PKType = (PK, RK)
    override def serializePK(pk: PKType): util.Map[String, AttributeValue] =
      Map(pkField -> PK.write(pk._1), rkField -> RK.write(pk._2)).asJava

    override def extract(av: util.Map[String, AttributeValue]): DynamoFormat.Result[PKType] = {
      val pk = Option(av.get(pkField)).fold[DynamoFormat.Result[PK]](Left(DynamoAttributeError.AttributeIsNull))(PK.read)
      val rk = Option(av.get(rkField)).fold[DynamoFormat.Result[RK]](Left(DynamoAttributeError.AttributeIsNull))(RK.read)
      pk -> rk mapN { _ -> _ }
    }

    override def schema: List[(String, ScalarAttributeType)] =
      pkField -> PK.attributeType :: rkField -> RK.attributeType :: Nil
  }
}
