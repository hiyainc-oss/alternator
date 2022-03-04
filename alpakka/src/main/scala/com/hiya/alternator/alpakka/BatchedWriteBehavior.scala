package com.hiya.alternator.alpakka

import akka.Done
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.stream.alpakka.dynamodb.DynamoDbOp
import com.hiya.alternator.Table.{AV, PK}
import com.hiya.alternator.alpakka.AlpakkaException.RetriesExhausted
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, BatchWriteItemRequest, BatchWriteItemResponse}
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

import java.util.{Map => JMap}
import scala.collection.compat._
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}


object BatchedWriteBehavior extends internal.BatchedBehavior {

  private[alternator] final case class WriteBuffer(queue: Queue[(Option[AV], List[Ref])], retries: Int)
    extends internal.BatchedBehavior.BufferItemBase[WriteBuffer] {
    override def withRetries(retries: Int): WriteBuffer = copy(retries = retries)
  }

  private[alternator] final case class WriteRequest(pk: PK, value: Option[AV])

  override protected type Request = WriteRequest
  override type Result = Done
  override protected type BufferItem = WriteBuffer
  override protected type FutureResult = BatchWriteItemResponse

  private class AwsClientAdapter(client: DynamoDbAsyncClient) {
    private def isSubMapOf(small: JMap[String, AttributeValue], in: JMap[String, AttributeValue]): Boolean =
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
                               retryPolicy: BatchRetryPolicy,
                               monitoring: BatchMonitoring
                             )(
                               ctx: ActorContext[BatchedRequest],
                               scheduler: TimerScheduler[BatchedRequest]
                             ) extends BaseBehavior(ctx, scheduler, maxWait, retryPolicy, monitoring, 25) {

    protected override def sendSuccess(futureResult: BatchWriteItemResponse, keys: List[PK], buffer: Buffer): (List[PK], List[PK], Buffer) = {
      val (success, failed) = client.processResult(keys, futureResult)

      val (buffer2, reschedule) = success.foldLeft(buffer -> List.empty[PK]) { case ((buffer, reschedule), key) =>
        val (refs2, bufferItem) = buffer(key).queue.dequeue
        sendResult(refs2._2, Success(Done))

        if (bufferItem.isEmpty) (buffer - key) -> reschedule
        else buffer.updated(key, WriteBuffer(bufferItem, 0)) -> (key :: reschedule)
      }

      (failed, reschedule, buffer2)
    }


    protected override def sendRetriesExhausted(cause: Exception, buffer: Buffer, pk: PK, item: WriteBuffer): Buffer = {
      val (refs, bufferItem) = item.queue.dequeue
      sendResult(refs._2, Failure(RetriesExhausted(cause)))

      if (bufferItem.isEmpty) buffer - pk
      else buffer.updated(pk, WriteBuffer(bufferItem, 0))
    }

    protected def sendFailure(keys: List[PK], buffer: BatchedWriteBehavior.Buffer, ex: Throwable): Buffer = {
      keys.foldLeft(buffer) { case (buffer, key) =>
        val (refs2, bufferItem) = buffer(key).queue.dequeue
        sendResult(refs2._2, Failure(ex))

        if (bufferItem.isEmpty) buffer - key
        else buffer.updated(key, WriteBuffer(bufferItem, 0))
      }
    }

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

    override protected def receive(req: WriteRequest, ref: Ref, buffer: Buffer): (List[PK], Buffer) = {
      val key = req.pk

      buffer.get(key) match {
        case Some(elem) =>
          Nil -> buffer.updated(key, elem.copy(queue = elem.queue.enqueue(req.value -> List(ref))))

        case None =>
          List(key) -> buffer.updated(key, WriteBuffer(Queue(req.value -> List(ref)), 0))
      }
    }
  }

  /**
   * DynamoDB batched writer
   *
   * The actor waits for the maximum size of write requests (25) or maxWait time before created a batched write
   * request. If the requests fails with a retryable error the elements will be rescheduled later (using the given
   * retryPolicy). Unprocessed items are rescheduled similarly.
   *
   * The received requests are deduplicated, only the last write to the key is executed.
   */
  def apply(
             client: DynamoDbAsyncClient,
             maxWait: FiniteDuration,
             retryPolicy: BatchRetryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy(),
             monitoring: BatchMonitoring = BatchMonitoring.Disabled
           ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new WriteBehavior(new AwsClientAdapter(client), maxWait, retryPolicy, monitoring)(ctx, scheduler).behavior(Queue.empty, Map.empty, None)
      }
    }
}
