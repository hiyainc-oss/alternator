package com.hiya.alternator

trait Client {
  type PKClient[V, PK]
  type RKClient[V, PK, RK] <: PKClient[V, (PK, RK)]

  def createPkClient[V, PK](table: Table[V, PK]): PKClient[V, PK]
  def createRkClient[V, PK, RK](table: TableWithRangeKey[V, PK, RK]): RKClient[V, PK, RK]
}
