package com.hiya.alternator.util

import akka.actor.ClassicActorSystemProvider
import com.hiya.alternator.alpakka.AlpakkaTable
import com.hiya.alternator.testkit.Timeout
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext

abstract class TableConfig[Data] {
  type Key
  type TableType <: AlpakkaTable[Data, Key]

  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T)
                  (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T
  def table(name: String, client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider): TableType
}
