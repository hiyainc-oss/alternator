package com.hiya.alternator.alpakka

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.scaladsl.{Flow, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.hiya.alternator.Table.AV
import com.hiya.alternator._

import scala.concurrent.Future

trait AlpakkaTableOps[V, PK] extends crud.TableOps[V, PK, Future, Source[*, NotUsed]] {

  def batchedGet(key: PK)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[DynamoFormat.Result[V]]]
  def batchedPut(value: V)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Done]
  def batchedDelete[T](value: T)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler, T : ItemMagnet[T, V, PK]): Future[Done]

  def batchedGetFlow(parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[PK, Option[DynamoFormat.Result[V]], NotUsed]
  def batchedGetFlowUnordered[PT](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[(PK, PT), (Option[DynamoFormat.Result[V]], PT), NotUsed]
}

object AlpakkaTableOps {
  import Alpakka.parasitic
  import com.hiya.alternator.Table.PK

  private def sendRead[V](pk: PK, deserializer: AV => DynamoFormat.Result[V])(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[DynamoFormat.Result[V]]] = {
    actorRef
      .ask((ref: BatchedReadBehavior.Ref) =>
        BatchedReadBehavior.Req(pk, ref)
      )
      .flatMap(result => Future.fromTry(result.map(_.map(deserializer))))
  }

  sealed trait ReadRequest[V] {
    def send()(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[V]
  }

  final case class ReadRequestWoPT[V](pk: PK, deserializer: AV => DynamoFormat.Result[V]) extends ReadRequest[Option[DynamoFormat.Result[V]]] {
    def send()(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[Option[DynamoFormat.Result[V]]] =
      sendRead(pk, deserializer)
  }

  final case class ReadRequestWPT[V, PT](pk: PK, deserializer: AV => DynamoFormat.Result[V], pt: PT) extends ReadRequest[(Option[DynamoFormat.Result[V]], PT)] {
    def send()(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[(Option[DynamoFormat.Result[V]], PT)] = {
      sendRead(pk, deserializer).map(_ -> pt)
    }

  }

  final case class WriteRequest[V](pk: PK, value: Option[AV], ret: V) {
    def send()(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Future[V] =
      actorRef
        .ask((ref: BatchedWriteBehavior.Ref) =>
          BatchedWriteBehavior.Req(BatchedWriteBehavior.WriteRequest(pk, value), ref)
        )
        .flatMap(result => Future.fromTry { result.map(_ => ret) })
  }


}
