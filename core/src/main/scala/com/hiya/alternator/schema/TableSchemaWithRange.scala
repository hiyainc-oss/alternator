package com.hiya.alternator.schema

import com.hiya.alternator.{Client, TableWithRangeKey, TableWithRangeKeyLike}

abstract class TableSchemaWithRange[V](
  serializeValue: RootDynamoFormat[V],
  val pkField: String,
  val rkField: String
) extends TableSchema[V](serializeValue) {

  type PK
  def PK: ScalarDynamoFormat[PK]

  type RK
  def RK: ScalarDynamoFormat[RK]

  override type IndexType = (PK, RK)

  override def withName(tableName: String): TableWithRangeKeyLike[Client.Missing, V, PK, RK] =
    new TableWithRangeKey(tableName, this)
}

object TableSchemaWithRange {
  type Aux[V, PKType, RKType] = TableSchemaWithRange[V] {
    type PK = PKType
    type RK = RKType
  }
}
