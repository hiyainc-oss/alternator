package com.hiya.alternator

import java.util

import akka.Done
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.stream.alpakka.dynamodb.DynamoDbOp
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, BatchWriteItemRequest, BatchWriteItemResponse}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.collection.compat._


object BatchedWriteBehavior extends internal.BatchedBehavior {
  import internal.BatchedBehavior._

  private [alternator] final case class WriteBuffer(queue: Queue[(Option[AV], List[Ref])], retries: Int)
  private [alternator] final case class WriteRequest(pk: PK, value: Option[AV], ref: Ref)

  override protected type Request = WriteRequest
  override type Result = Done
  override protected type Buffer = Map[PK, WriteBuffer]


  override protected type FutureResult = BatchWriteItemResponse
  override protected type FuturePassThru = List[PK]

  private class AwsClientAdapter(client: DynamoDbAsyncClient) {
    private def isSubMapOf(small: util.Map[String, AttributeValue], in: util.Map[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())

    def processResult(keys: List[PK], response: BatchWriteItemResponse): (List[PK], List[PK]) = {
      val allKeys = mutable.HashSet.from(keys)

      val unprocessedKeys = response.unprocessedItems().asScala.toList.flatMap { case (table, operations) =>
        val ops = operations.asScala
        val puts: mutable.Seq[PK] = ops
          .flatMap(op => Option(op.putRequest()))
          .map { item => allKeys.find { case (t, key) => t == table && isSubMapOf(key, item.item()) }.get }

        val deletes: mutable.Seq[PK] = ops
          .flatMap(op => Option(op.deleteRequest()))
          .map(table -> _.key())

        puts ++ deletes
      }

      allKeys --= unprocessedKeys

      allKeys.toList -> unprocessedKeys
    }

    def createQuery(key: List[(PK, Option[AV])]): Future[BatchWriteItemResponse] = {
      val request = BatchWriteItemRequest.builder()
        .requestItems(
          key
            .groupMap(_._1._1)(x => x._1._2 -> x._2)
            .view.mapValues(_.map({
                case (_, Some(value)) =>
                  model.WriteRequest.builder().putRequest(model.PutRequest.builder().item(value).build()).build()
                case (key, None) =>
                  model.WriteRequest.builder().deleteRequest(model.DeleteRequest.builder().key(key).build()).build()
              }).asJavaCollection)
            .toMap.asJava
        )
        .build()

      DynamoDbOp.batchWriteItem.execute(request)(client)
    }
  }

  private class WriteBehavior(
    client: AwsClientAdapter,
    maxWait: FiniteDuration,
    retryPolicy: RetryPolicy
  )(
    ctx: ActorContext[BatchedRequest],
    scheduler: TimerScheduler[BatchedRequest]
  ) extends BaseBehavior(ctx, scheduler, maxWait, 25, retryPolicy) {
    override protected def jobSuccess(futureResult: BatchWriteItemResponse, keys: FuturePassThru, buffer: Buffer): BatchedWriteBehavior.ProcessResult = {
      val (success, failed) = client.processResult(keys, futureResult)

      val buffer2 = success.foldLeft(buffer) { case (buffer, key) =>
        val (refs2, bufferItem) = buffer(key).queue.dequeue
        sendResult(refs2._2, Done)

        if (bufferItem.isEmpty) buffer - key
        else buffer.updated(key, WriteBuffer(bufferItem, 0))
      }

      val (buffer3, retries) = failed
        .foldLeft(buffer2 -> List.empty[(Int, PK)]) { case ((pending, retries), pk) =>
          val buffer = pending(pk)
          pending.updated(pk, buffer.copy(retries = buffer.retries + 1)) -> ((buffer.retries -> pk) :: retries)
        }

      ProcessResult(Nil, retries, buffer3, Nil)
    }

    override protected def jobFailure(ex: Throwable, pt: FuturePassThru, buffer: Buffer): BatchedWriteBehavior.ProcessResult = ???

    override protected def startJob(keys: List[PK], buffer: Buffer): (Future[BatchWriteItemResponse], List[PK], Buffer) = {
      // Collapse buffer: keep only the last value to write and all actorRefs
      val (buffer2, writes) = keys.foldLeft(buffer -> List.empty[(PK, Option[AV])]) { case ((buffer, writes), key) =>
        val values = buffer(key)
        val refs = values.queue.flatMap(_._2).toList
        val item = values.queue.last._1
        val buffer2 = buffer.updated(key, WriteBuffer(Queue(item -> refs), values.retries))
        buffer2 -> ((key -> item) :: writes)
      }

      (client.createQuery(writes), keys, buffer2)
    }

    override protected def receive(req: WriteRequest, buffer: Buffer): (List[PK], Buffer) = {
      ???
//      val key = req.key
//
//      buffer.get(key) match {
//        case Some(elem) =>
//          Nil -> buffer.updated(key, elem.copy(refs = req.ref :: elem.refs))
//
//        case None =>
//          List(key) -> buffer.updated(key, WriteBuffer(req.ref :: Nil, 0))
//      }
    }
  }

  def apply(
             client: DynamoDbAsyncClient,
             maxWait: FiniteDuration,
             retryPolicy: RetryPolicy
           ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new WriteBehavior(new AwsClientAdapter(client), maxWait, retryPolicy)(ctx, scheduler).behavior(Queue.empty, Map.empty)
      }
    }
}

