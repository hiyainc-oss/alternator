package com.hiya.alternator.cats.util

import akka.actor.ClassicActorSystemProvider
import cats.effect.IO
import com.hiya.alternator.cats.CatsTableOps
import com.hiya.alternator.testkit.Timeout
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext

abstract class TableConfig[Data, Key, TableType <: CatsTableOps[IO, Data, Key]] {
  def createData(i: Int, v: Option[Int] = None): (Key, Data)
  def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T)
                  (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T
  def table(name: String, client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider): TableType
}
