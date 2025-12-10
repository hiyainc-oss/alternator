package com.hiya.alternator.schema

abstract class TableSchemaWithPartition[V](
  serializeValue: RootDynamoFormat[V],
  val pkField: String
) extends TableSchema[V](serializeValue) {

  type PK
  def PK: ScalarDynamoFormat[PK]

  override type IndexType = PK
}

object TableSchemaWithPartition {
  type Aux[V, PKType] = TableSchemaWithPartition[V] {
    type PK = PKType
  }
}
