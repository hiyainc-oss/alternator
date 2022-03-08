package com.hiya.alternator.alpakka.internal

import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{BidiFlow, Flow, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.hiya.alternator.Table.AV
import com.hiya.alternator._
import com.hiya.alternator.alpakka.{Alpakka, AlpakkaTable, BatchedReadBehavior, BatchedWriteBehavior}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.Future


class AlpakkaTableInternal[V, PK](val table: Table[V, PK])(implicit client: DynamoDbAsyncClient, system: ClassicActorSystemProvider)
  extends AlpakkaTable[V, PK]
{
  final def get(pk: PK): Future[Option[DynamoFormat.Result[V]]] = {
    import com.hiya.alternator.alpakka.Alpakka.parasitic
    DynamoDb.single(table.get(pk).build()).map(table.deserialize)
  }

  final def scan(segment: Option[Segment] = None): Source[DynamoFormat.Result[V], NotUsed] = {
    DynamoDb.source(table.scan(segment).build()).mapConcat(table.deserialize)
  }

  final val readerFlow: BidiFlow[PK, GetItemRequest.Builder, GetItemResponse, DynamoFormat.Result[V], NotUsed] = {
    BidiFlow.fromFlows(
      Flow[PK].map(table.get),
      Flow[GetItemResponse].map(response => table.schema.serializeValue.readFields(response.item()))
    )
  }

  final def readRequest(key: PK): AlpakkaTable.ReadRequest[Option[DynamoFormat.Result[V]]] =
    AlpakkaTable.ReadRequestWoPT(
      table.tableName -> table.schema.serializePK(key),
      table.deserialize(_:AV)
    )

  final def readRequest[PT](key: PK, pt: PT): AlpakkaTable.ReadRequest[(Option[DynamoFormat.Result[V]], PT)] =
    AlpakkaTable.ReadRequestWPT(
      table.tableName -> table.schema.serializePK(key),
      table.deserialize(_:AV),
      pt
    )

  final def batchedGet(key: PK)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[DynamoFormat.Result[V]]] = {
    readRequest(key).send()
  }

  final def batchedGetFlow(parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[PK, Option[DynamoFormat.Result[V]], NotUsed] =
    Flow[PK].mapAsync(parallelism)(batchedGet)

  final def batchedGetFlowUnordered[PT](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[(PK, PT), (Option[DynamoFormat.Result[V]], PT), NotUsed] =
    Flow[(PK, PT)].mapAsyncUnordered(parallelism) { case (key, pt) =>
      batchedGet(key).map(_ -> pt)(Alpakka.parasitic)
    }

  final def batchGet(values: Seq[PK]): Future[BatchGetItemResponse] = {
    DynamoDb.single(table.batchGet(values).build())
  }


  final def putRequest(item: V): AlpakkaTable.WriteRequest[Done] = {
    putRequest(item, Done)
  }

  final def putRequest[PT](item: V, pt: PT): AlpakkaTable.WriteRequest[PT] = {
    val itemValue = table.schema.serializeValue.writeFields(item)
    AlpakkaTable.WriteRequest(table.tableName -> table.schema.serializePK(table.schema.extract(item)), Some(itemValue), pt)
  }

  final def put(value: V): Future[Unit] = {
    import Alpakka.parasitic
    DynamoDb.single(table.put(value).build()).map(_ => ())
  }

  final def batchPut(values: Seq[V]): Future[BatchWriteItemResponse] = {
    DynamoDb.single(table.batchPut(values).build())
  }

  final def batchedPut(value: V)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Done] =
    putRequest(value).send()


  final def delete(key: PK): Future[Unit] = {
    import Alpakka.parasitic
    DynamoDb.single(table.delete(key).build()).map(_ => ())
  }

  final def deleteRequest[T](item: T)(implicit T : table.ItemMagnet[T]): AlpakkaTable.WriteRequest[Done] = {
    deleteRequest(item, Done)
  }

  final def deleteRequest[T, PT](item: T, pt: PT)(implicit T : table.ItemMagnet[T]): AlpakkaTable.WriteRequest[PT] = {
    AlpakkaTable.WriteRequest(table.tableName -> table.schema.serializePK(T.key(item)), None, pt)
  }

  final def batchedDelete[T : table.ItemMagnet](value: T)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Done] =
    deleteRequest(value).send()

  final def batchDelete[T : table.ItemMagnet](values: Seq[T]): Future[BatchWriteItemResponse] = {
    DynamoDb.single(table.batchDelete(values).build())
  }

  override final def batchWrite(values: Seq[Either[PK, V]]): Future[BatchWriteItemResponse] =
    DynamoDb.single(table.batchWrite(values).build())
}
