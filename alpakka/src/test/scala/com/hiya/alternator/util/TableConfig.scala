package com.hiya.alternator.util

import com.hiya.alternator.Table
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

abstract class TableConfig[Data] {
  type Key
  type TableType <: Table[Data, Key]

  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T): T
}
