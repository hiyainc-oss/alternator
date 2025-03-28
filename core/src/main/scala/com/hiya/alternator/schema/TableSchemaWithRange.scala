package com.hiya.alternator.schema

import com.hiya.alternator.{DynamoDBClient, TableWithRange}

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

  override def withName(tableName: String): TableWithRange[DynamoDBClient.Missing, V, PK, RK] =
    new TableWithRange(DynamoDBClient.Missing, tableName, this)
}

object TableSchemaWithRange {
  type Aux[V, PKType, RKType] = TableSchemaWithRange[V] {
    type PK = PKType
    type RK = RKType
  }
}
