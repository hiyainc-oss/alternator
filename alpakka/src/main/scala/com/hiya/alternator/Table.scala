package com.hiya.alternator

import akka.{Done, NotUsed}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.Timeout
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.{ExecutionContext, Future}

class Table[V, PK](name: String, schema: TableSchema.Aux[V, PK]) {
  final def orThrow[T](x: DynamoFormat.Result[T]): T = x match {
    case Left(error) => throw Table.DynamoDBException(error)
    case Right(value) => value
  }

  final def getBuilder(pk: PK): GetItemRequest.Builder =
    GetItemRequest.builder().key(schema.serializePK(pk)).tableName(name)

  final def get(pk: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Option[V]] = {
    val ret: Future[GetItemResponse] = DynamoDb.single(getBuilder(pk).build())
    ret.map { response =>
      if (response.hasItem) Some(orThrow(schema.serializeValue.readFields(response.item())))
      else None
    }(ExecutionContext.parasitic)
  }

  final def getFlow[M](flow: Flow[GetItemRequest, GetItemResponse, M]): Flow[PK, DynamoFormat.Result[V], M] = {
    Flow[PK]
      .map(getBuilder(_).build())
      .viaMat(flow)(Keep.right)
      .map(response => schema.serializeValue.readFields(response.item()))
  }


  final def putBuilder(item: V): PutItemRequest.Builder =
    PutItemRequest.builder().item(schema.serializeValue.writeFields(item)).tableName(name)

  final def put(value: V)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Done] = {
    val ret: Future[PutItemResponse] = DynamoDb.single(putBuilder(value).build())
    ret.map(_ => Done)(ExecutionContext.parasitic)
  }


  final def deleteBuilder(key: PK): DeleteItemRequest.Builder =
    DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(name)

  final def delete(key: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Done] = {
    val ret: Future[DeleteItemResponse] = DynamoDb.single(deleteBuilder(key).build())
    ret.map(_ => Done)(ExecutionContext.parasitic)
  }

  final def batchedGet(key: PK)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[V]] = {
    actorRef.ask((ref: BatchedReadBehavior.Ref) =>
      BatchedReadBehavior.Req(BatchedReadBehavior.ReadRequest(name -> schema.serializePK(key), ref))
    ).map(_.map(x => orThrow(schema.serializeValue.readFields(x))))(ExecutionContext.parasitic)
  }

  final def batchedGetFlow(parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[PK, Option[V], NotUsed] = {
    Flow[PK].mapAsync(parallelism) { key =>
      actorRef.ask((ref: BatchedReadBehavior.Ref) =>
        BatchedReadBehavior.Req(BatchedReadBehavior.ReadRequest(name -> schema.serializePK(key), ref))
      ).map(_.map(x => orThrow(schema.serializeValue.readFields(x))))(ExecutionContext.parasitic)
    }
  }

  final def batchedGetFlowUnordered[PT](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[(PK, PT), (Option[V], PT), NotUsed] = {
    Flow[(PK, PT)].mapAsyncUnordered(parallelism) { case (key, pt) =>
      actorRef.ask((ref: BatchedReadBehavior.Ref) =>
        BatchedReadBehavior.Req(BatchedReadBehavior.ReadRequest(name -> schema.serializePK(key), ref))
      ).map(ret => ret.map(x => orThrow(schema.serializeValue.readFields(x))) -> pt)(ExecutionContext.parasitic)
    }
  }

}

object Table {
  sealed trait WriterRequest[P, PT]

  final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

  def tableWithPK[V](name: String)(
    implicit tableSchema: TableSchema[V]
  ): Table[V, tableSchema.PKType] = new Table[V, tableSchema.PKType](name, tableSchema)

}
