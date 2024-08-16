package com.hiya.alternator.schema

import cats.instances.either._
import cats.syntax.apply._
import com.hiya.alternator.{Client, Table, TableLike}

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

abstract class TableSchema[V](val serializeValue: RootDynamoFormat[V]) {
  type IndexType

  def serializePK[AV: AttributeValue](pk: IndexType): JMap[String, AV]
  def extract[AV: AttributeValue](av: JMap[String, AV]): DynamoFormat.Result[IndexType]
  def extract(value: V): IndexType
  def schema: List[(String, ScalarType)]

  def withName(tableName: String): TableLike[Client.Missing, V, IndexType] =
    new Table[V, IndexType](tableName, this)

}

object TableSchema {
  type Aux[V, PK] = TableSchema[V] { type IndexType = PK }

  def schemaWithPK[V, PK](pkField: String, extractPK: V => PK)(implicit
    PK: ScalarDynamoFormat[PK],
    V: RootDynamoFormat[V]
  ): TableSchema.Aux[V, PK] = new TableSchema[V](V) {
    override type IndexType = PK

    override def serializePK[AV: AttributeValue](pk: PK): JMap[String, AV] =
      Map(pkField -> PK.write(pk)).asJava

    override def extract[AV: AttributeValue](av: JMap[String, AV]): DynamoFormat.Result[PK] = {
      Option(av.get(pkField)).fold[DynamoFormat.Result[PK]](Left(DynamoAttributeError.AttributeIsNull))(PK.read)
    }

    override def extract(value: V): PK = extractPK(value)

    override def schema: List[(String, ScalarType)] = pkField -> PK.attributeType :: Nil
  }

  def schemaWithRK[V, PKType, RKType](pkField: String, rkField: String, extractPK: V => (PKType, RKType))(implicit
    V: RootDynamoFormat[V],
    PKType: ScalarDynamoFormat[PKType],
    RKType: ScalarDynamoFormat[RKType]
  ): TableSchemaWithRange.Aux[V, PKType, RKType] = new TableSchemaWithRange[V](V, pkField, rkField) {
    override type PK = PKType
    override val PK = PKType
    override type RK = RKType
    override val RK = RKType

    override def serializePK[AV: AttributeValue](pk: IndexType): JMap[String, AV] =
      Map(this.pkField -> PK.write(pk._1), this.rkField -> RK.write(pk._2)).asJava

    override def extract[AV: AttributeValue](av: JMap[String, AV]): DynamoFormat.Result[IndexType] = {
      val pk =
        Option(av.get(this.pkField)).fold[DynamoFormat.Result[PK]](Left(DynamoAttributeError.AttributeIsNull))(PK.read)
      val rk =
        Option(av.get(this.rkField)).fold[DynamoFormat.Result[RK]](Left(DynamoAttributeError.AttributeIsNull))(RK.read)
      pk -> rk mapN { _ -> _ }
    }

    override def extract(value: V): IndexType = extractPK(value)

    override def schema: List[(String, ScalarType)] =
      this.pkField -> PK.attributeType :: this.rkField -> RK.attributeType :: Nil
  }

}
