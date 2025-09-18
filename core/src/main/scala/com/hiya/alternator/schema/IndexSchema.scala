package com.hiya.alternator.schema
import com.hiya.alternator.schema.DynamoFormat.Result

import java.util

final case class IndexSchema[V, PK](
  indexName: String,
  schema: TableSchemaWithPartition.Aux[V, PK]
)

object IndexSchema {
  def onTable[V, PKType](tableSchema: TableSchema.Aux[V, _])(
    indexName: String,
    pkField: String,
    extractPK: V => PKType
  )(implicit
    PKType: ScalarDynamoFormat[PKType]
  ): IndexSchema[V, PKType] = {
    val underlying = TableSchema.schemaWithPK(pkField, extractPK)(PKType, tableSchema.serializeValue)
    val schema = new TableSchemaWithPartition[V](tableSchema.serializeValue, pkField) {

      override type PK = PKType
      override def PK: ScalarDynamoFormat[PKType] = PKType

      override def serializePK[AV: AttributeValue](pk: PKType): util.Map[String, AV] = underlying.serializePK(pk)
      override def extract[AV: AttributeValue](av: util.Map[String, AV]): Result[PKType] = underlying.extract(av)
      override def extract(value: V): PKType = underlying.extract(value)
      override def schema: List[(String, ScalarType)] = underlying.schema
    }

    IndexSchema(indexName, schema)
  }
}

final case class IndexSchemaWithRange[V, PKType, RKType](
  indexName: String,
  schema: TableSchemaWithRange.Aux[V, PKType, RKType]
)

object IndexSchemaWithRange {
  def onTable[V, PKType, RKType](tableSchema: TableSchema.Aux[V, _])(
    indexName: String,
    pkField: String,
    rkField: String,
    extractKey: V => (PKType, RKType)
  )(implicit
    PKType: ScalarDynamoFormat[PKType],
    RKType: ScalarDynamoFormat[RKType]
  ): IndexSchemaWithRange[V, PKType, RKType] = {
    val underlying = TableSchema.schemaWithRK(pkField, rkField, extractKey)(tableSchema.serializeValue, PKType, RKType)
    val schema = new TableSchemaWithRange[V](tableSchema.serializeValue, pkField, rkField) {

      override type PK = PKType
      override def PK: ScalarDynamoFormat[PKType] = PKType

      override type RK = RKType
      override def RK: ScalarDynamoFormat[RKType] = RKType

      override def serializePK[AV: AttributeValue](pk: IndexType): util.Map[String, AV] = underlying.serializePK(pk)
      override def extract[AV: AttributeValue](av: util.Map[String, AV]): Result[IndexType] = underlying.extract(av)
      override def extract(value: V): IndexType = underlying.extract(value)
      override def schema: List[(String, ScalarType)] = underlying.schema
    }

    IndexSchemaWithRange(indexName, schema)
  }

}
