package com.hiya.alternator

import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Flow, Keep}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

import scala.concurrent.{ExecutionContext, Future}

class Table[V, PK](name: String, schema: TableSchema.Aux[V, PK]) {
  final def getBuilder(pk: PK): GetItemRequest.Builder =
    GetItemRequest.builder().key(schema.serializePK(pk)).tableName(name)

  final def get(pk: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[DynamoFormat.Result[V]] = {
    val ret: Future[GetItemResponse] = DynamoDb.single(getBuilder(pk).build())
    ret.map(response => schema.V.readFields(response.item()))(ExecutionContext.parasitic)
  }


  final def getFlow[M](flow: Flow[GetItemRequest, GetItemResponse, M]): Flow[PK, DynamoFormat.Result[V], M] = {
    Flow[PK]
      .map(getBuilder(_).build())
      .viaMat(flow)(Keep.right)
      .map(response => schema.V.readFields(response.item()))
  }

  final def put(pk: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[DynamoFormat.Result[V]] = {
    val ret: Future[GetItemResponse] = DynamoDb.single(getBuilder(pk).build())
    ret.map(response => schema.V.readFields(response.item()))(ExecutionContext.parasitic)
  }
}

object Table {
  def tableWithPK[V](name: String)(
    implicit tableSchema: TableSchema[V]
  ): Table[V, tableSchema.PKType] = new Table[V, tableSchema.PKType](name, tableSchema)

}
