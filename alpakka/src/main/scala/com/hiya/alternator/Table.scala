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

import cats.syntax.option._
import cats.syntax.traverse._
import cats.instances.option._
import cats.instances.future._

import scala.concurrent.{ExecutionContext, Future}

class Table[V, PK](name: String, schema: TableSchema.Aux[V, PK]) {
  final def orFail[T](x: DynamoFormat.Result[T]): Future[T] = x match {
    case Left(error) => Future.failed(Table.DynamoDBException(error))
    case Right(value) => Future.successful(value)
  }

  final def getBuilder(pk: PK): GetItemRequest.Builder =
    GetItemRequest.builder().key(schema.serializePK(pk)).tableName(name)

  final def get(pk: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Option[V]] = {
    import Table.parasitic

    val ret: Future[GetItemResponse] = DynamoDb.single(getBuilder(pk).build())
    ret.flatMap { response =>
      if (response.hasItem) orFail(schema.serializeValue.readFields(response.item())).map(_.some)(Table.parasitic)
      else Future.successful(None)
    }
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
    import Table.parasitic

    val ret: Future[PutItemResponse] = DynamoDb.single(putBuilder(value).build())
    ret.map(_ => Done)
  }

  final def deleteBuilder(key: PK): DeleteItemRequest.Builder =
    DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(name)

  final def delete(key: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Done] = {
    import Table.parasitic

    val ret: Future[DeleteItemResponse] = DynamoDb.single(deleteBuilder(key).build())
    ret.map(_ => Done)
  }

  final def batchedGet(key: PK)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[V]] = {
    import Table.parasitic

    actorRef.ask((ref: BatchedReadBehavior.Ref) =>
      BatchedReadBehavior.Req(BatchedReadBehavior.ReadRequest(name -> schema.serializePK(key), ref))
    ).flatMap(_.traverse(x => orFail(schema.serializeValue.readFields(x))))
  }

  final def batchedGetFlow(parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[PK, Option[V], NotUsed] = {
    import Table.parasitic

    Flow[PK].mapAsync(parallelism) { key =>
      actorRef.ask((ref: BatchedReadBehavior.Ref) =>
        BatchedReadBehavior.Req(BatchedReadBehavior.ReadRequest(name -> schema.serializePK(key), ref))
      ).flatMap(_.traverse(x => orFail(schema.serializeValue.readFields(x))))
    }
  }

  final def batchedGetFlowUnordered[PT](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[(PK, PT), (Option[V], PT), NotUsed] = {
    import Table.parasitic

    Flow[(PK, PT)].mapAsyncUnordered(parallelism) { case (key, pt) =>
      actorRef.ask((ref: BatchedReadBehavior.Ref) =>
        BatchedReadBehavior.Req(BatchedReadBehavior.ReadRequest(name -> schema.serializePK(key), ref))
      ).flatMap(ret => ret.traverse(x => orFail(schema.serializeValue.readFields(x))).map(_ -> pt))
    }
  }

}

object Table {
  private implicit lazy val parasitic: ExecutionContext = {
    // The backport is present in akka, so we will just use it by reflection
    // It probably will not change, as it is a stable internal api
    val q = akka.dispatch.ExecutionContexts
    val clazz = q.getClass
    val field = clazz.getDeclaredField("parasitic")
    field.setAccessible(true)
    val ret = field.get(q).asInstanceOf[ExecutionContext]
    field.setAccessible(false)
    ret
  }

  sealed trait WriterRequest[P, PT]

  final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

  def tableWithPK[V](name: String)(
    implicit tableSchema: TableSchema[V]
  ): Table[V, tableSchema.PKType] = new Table[V, tableSchema.PKType](name, tableSchema)

}
