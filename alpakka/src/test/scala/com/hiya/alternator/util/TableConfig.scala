package com.hiya.alternator.util

import com.hiya.alternator.Table
import com.hiya.alternator.testkit.Timeout
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext

abstract class TableConfig[Data] {
  type Key
  type TableType <: Table[Data, Key]

  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T)(implicit ec: ExecutionContext, timeout: Timeout): T
  def table(name: String): TableType
}
