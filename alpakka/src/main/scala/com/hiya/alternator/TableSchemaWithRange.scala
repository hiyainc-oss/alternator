package com.hiya.alternator

abstract class TableSchemaWithRange[V](serializeValue: CompoundDynamoFormat[V], val pkField: String, val rkField: String)
  extends TableSchema[V](serializeValue) {

  type PK
  def PK: ScalarDynamoFormat[PK]

  type RK
  def RK: ScalarDynamoFormat[RK]

  override type IndexType = (PK, RK)

}

object TableSchemaWithRange {
  type Aux[V, PKType, RKType] = TableSchemaWithRange[V] {
    type PK = PKType
    type RK = RKType
  }
}
