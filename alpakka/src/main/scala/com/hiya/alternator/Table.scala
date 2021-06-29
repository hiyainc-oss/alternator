package com.hiya.alternator

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.{BidiFlow, Flow}
import akka.util.Timeout
import akka.{Done, NotUsed}
import cats.instances.option._
import cats.syntax.traverse._
import com.hiya.alternator.internal.BatchedBehavior
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemResponse, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Table[V, PK](val name: String, schema: TableSchema.Aux[V, PK]) {
  final def getBuilder(pk: PK): GetItemRequest.Builder =
    GetItemRequest.builder().key(schema.serializePK(pk)).tableName(name)

  final def get(pk: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Option[V]] = {
    import Table.parasitic

    val ret: Future[GetItemResponse] = DynamoDb.single(getBuilder(pk).build())
    ret.flatMap(result => Future.fromTry(deserialize(result)))
  }

  final def deserialize(response: BatchedBehavior.AV): Try[V] = {
    Table.orFail(schema.serializeValue.readFields(response))
  }

  final def deserialize(response: GetItemResponse): Try[Option[V]] = {
    if (response.hasItem) Option(response.item()).traverse(deserialize)
    else Success(None)
  }

  final val readerFlow: BidiFlow[PK, GetItemRequest.Builder, GetItemResponse, DynamoFormat.Result[V], NotUsed] = {
    BidiFlow.fromFlows(
      Flow[PK].map(getBuilder),
      Flow[GetItemResponse].map(response => schema.serializeValue.readFields(response.item()))
    )
  }

  final def readRequest(key: PK): Table.ReadRequest[Option[V]] =
    Table.ReadRequestWoPT(
      name -> schema.serializePK(key),
      deserialize(_:BatchedBehavior.AV)
    )

  final def readRequest[PT](key: PK, pt: PT): Table.ReadRequest[(Option[V], PT)] =
    Table.ReadRequestWPT(
      name -> schema.serializePK(key),
      deserialize(_:BatchedBehavior.AV),
      pt
    )

  final def batchedGet(key: PK)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[V]] = {
    readRequest(key).send()
  }

  final def batchedGetFlow(parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[PK, Option[V], NotUsed] =
    Flow[PK].mapAsync(parallelism)(batchedGet)

  final def batchedGetFlowUnordered[PT](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[(PK, PT), (Option[V], PT), NotUsed] =
    Flow[(PK, PT)].mapAsyncUnordered(parallelism) { case (key, pt) =>
      batchedGet(key).map(_ -> pt)(Table.parasitic)
    }

  final def putBuilder(item: V): PutItemRequest.Builder =
    PutItemRequest.builder().item(schema.serializeValue.writeFields(item)).tableName(name)

  final def put(value: V)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Done] = {
    import Table.parasitic

    val ret: Future[PutItemResponse] = DynamoDb.single(putBuilder(value).build())
    ret.map(_ => Done)
  }

  final def batchedPut(value: V)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Done] =
    putRequest(value).send()

  final def putRequest(item: V): Table.WriteRequest[Done] = {
    val itemValue = schema.serializeValue.writeFields(item)
    Table.WriteRequest(name -> schema.serializePK(schema.extract(item)), Some(itemValue), Done)
  }

  final def putRequest[PT](item: V, pt: PT): Table.WriteRequest[PT] = {
    val itemValue = schema.serializeValue.writeFields(item)
    Table.WriteRequest(name -> schema.serializePK(schema.extract(item)), Some(itemValue), pt)
  }

  final def deleteBuilder(key: PK): DeleteItemRequest.Builder =
    DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(name)

  final def delete(key: PK)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[Done] = {
    import Table.parasitic

    val ret: Future[DeleteItemResponse] = DynamoDb.single(deleteBuilder(key).build())
    ret.map(_ => Done)
  }

  sealed trait ItemMagnet[T] {
    def key(t: T): PK
  }

  object ItemMagnet {
    implicit object WholeItem extends ItemMagnet[V] {
      override def key(t: V): PK = schema.extract(t)
    }

    implicit object ItemKey extends ItemMagnet[PK] {
      override def key(t: PK): PK = t
    }
  }

  final def deleteRequest[T, PT](item: T, pt: PT = Done)(implicit T : ItemMagnet[T]): Table.WriteRequest[PT] = {
    Table.WriteRequest(name -> schema.serializePK(T.key(item)), None, pt)
  }

  final def batchedDelete[T : ItemMagnet](value: T)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Done] =
    deleteRequest(value).send()

}

object Table {
  private [alternator] implicit lazy val parasitic: ExecutionContext = {
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

  final def orFail[T](x: DynamoFormat.Result[T]): Try[T] = x match {
    case Left(error) => Failure(Table.DynamoDBException(error))
    case Right(value) => Success(value)
  }

  def orderedReader[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[ReadRequest[V], V, NotUsed] =
    Flow[ReadRequest[V]].mapAsync(parallelism)(_.send())

  def unorderedReader[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[ReadRequest[V], V, NotUsed] =
    Flow[ReadRequest[V]].mapAsyncUnordered(parallelism)(_.send())

  private def sendRead[V](pk: BatchedBehavior.PK, deserializer: BatchedBehavior.AV => Try[V])(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[V]] =
    actorRef
      .ask((ref: BatchedReadBehavior.Ref) =>
        BatchedReadBehavior.Req(pk, ref)
      )
      .flatMap(result => Future.fromTry { result.flatMap(_.traverse(deserializer)) })

  sealed trait ReadRequest[V] {
    def send()(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[V]
  }

  final case class ReadRequestWoPT[V](pk: BatchedBehavior.PK, deserializer: BatchedBehavior.AV => Try[V]) extends ReadRequest[Option[V]] {
    def send()(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[V]] =
      sendRead(pk, deserializer)
  }

  final case class ReadRequestWPT[V, PT](pk: BatchedBehavior.PK, deserializer: BatchedBehavior.AV => Try[V], pt: PT) extends ReadRequest[(Option[V], PT)] {
    def send()(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[(Option[V], PT)] =
      sendRead(pk, deserializer).map(_ -> pt)

  }

  final case class WriteRequest[V](pk: BatchedBehavior.PK, value: Option[BatchedBehavior.AV], ret: V) {
    def send()(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[V] =
      actorRef
        .ask((ref: BatchedWriteBehavior.Ref) =>
          BatchedWriteBehavior.Req(BatchedWriteBehavior.WriteRequest(pk, value), ref)
        )
        .flatMap(result => Future.fromTry { result.map(_ => ret) })
  }

  def orderedWriter[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[WriteRequest[V], V, NotUsed] =
    Flow[WriteRequest[V]].mapAsync(parallelism)(_.send())

  def unorderedWriter[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[WriteRequest[V], V, NotUsed] =
    Flow[WriteRequest[V]].mapAsyncUnordered(parallelism)(_.send())

  final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

  def tableWithPK[V](name: String)(
    implicit tableSchema: TableSchema[V]
  ): Table[V, tableSchema.IndexType] = new Table[V, tableSchema.IndexType](name, tableSchema)

  def tableWithRK[V](name: String)(
    implicit tableSchema: TableSchemaWithRange[V]
  ): TableWithRange[V, tableSchema.PK, tableSchema.RK] =
    new TableWithRange[V, tableSchema.PK, tableSchema.RK](name, tableSchema)

}
