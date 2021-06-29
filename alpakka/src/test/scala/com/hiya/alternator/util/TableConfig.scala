package com.hiya.alternator.util

import com.hiya.alternator.Table
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

abstract class TableConfig[Data] {
  type Key

  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[T](client: DynamoDbAsyncClient)(f: Table[Data, Key] => T): T
}
