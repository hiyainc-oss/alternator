package com.hiya.alternator.testkit

import com.hiya.alternator.testkit.LocalDynamoDB.deleteTable
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.{Await, ExecutionContext, Future}


class WithRandomTable(client: DynamoDbAsyncClient, tableName: String, magnet: SchemaMagnet) {
  def future[U](thunk: String => U)(implicit ex: ExecutionContext): Future[U] = {
    LocalDynamoDB
      .createTable(client)(tableName)(magnet)
      .map(_ => thunk(tableName))
      .transformWith(ret => deleteTable(client)(tableName).transform(_ => ret))
  }

  def apply[U](thunk: String => U)(implicit timeout: Timeout, ex: ExecutionContext): U = {
    Await.result(future(thunk), timeout.value)
  }
}

