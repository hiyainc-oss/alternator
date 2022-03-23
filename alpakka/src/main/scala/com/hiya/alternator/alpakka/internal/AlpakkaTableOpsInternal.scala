package com.hiya.alternator.alpakka.internal

import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{BidiFlow, Flow, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.hiya.alternator._
import com.hiya.alternator.alpakka.{Alpakka, AlpakkaTableOps, BatchedReadBehavior, BatchedWriteBehavior}
import com.hiya.alternator.alpakka.stream._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.Future


class AlpakkaTableOpsInternal[V, PK](override val table: Table[V, PK])(implicit val client: DynamoDbAsyncClient, system: ClassicActorSystemProvider)
  extends AlpakkaTableOps[V, PK]
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


  final def batchedGet(key: PK)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[DynamoFormat.Result[V]]] = {
    table.readRequest(key).send()
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


  final def put(value: V): Future[Unit] = {
    import Alpakka.parasitic
    DynamoDb.single(table.put(value).build()).map(_ => ())
  }

  final def batchPut(values: Seq[V]): Future[BatchWriteItemResponse] = {
    DynamoDb.single(table.batchPut(values).build())
  }

  final def batchedPut(value: V)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Done] =
    table.putRequest(value).send()


  final def delete(key: PK): Future[Unit] = {
    import Alpakka.parasitic
    DynamoDb.single(table.delete(key).build()).map(_ => ())
  }

  final def batchedDelete[T](value: T)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler, T : ItemMagnet[T, V, PK]): Future[Done] = {
    table.deleteRequest(value).send()
  }

  final def batchDelete[T](values: Seq[T])(implicit T: ItemMagnet[T, V, PK]): Future[BatchWriteItemResponse] = {
    DynamoDb.single(table.batchDelete(values).build())
  }

  override final def batchWrite(values: Seq[Either[PK, V]]): Future[BatchWriteItemResponse] =
    DynamoDb.single(table.batchWrite(values).build())
}
