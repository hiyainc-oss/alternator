package com.hiya.alternator.testkit

import com.hiya.alternator.testkit.LocalDynamoDB.deleteTable
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.{Await, ExecutionContext, Future}


class WithTable(client: DynamoDbAsyncClient, tableName: String, magnet: SchemaMagnet) {
  def future[U](thunk: => U)(implicit ex: ExecutionContext): Future[U] = {
    LocalDynamoDB
      .createTable(client)(tableName)(magnet)
      .map(_ => thunk)
      .transformWith(ret => deleteTable(client)(tableName).transform(_ => ret))
  }

  def apply[U](thunk: => U)(implicit timeout: Timeout, ex: ExecutionContext): U = {
    Await.result(future(thunk), timeout.value)
  }


}
