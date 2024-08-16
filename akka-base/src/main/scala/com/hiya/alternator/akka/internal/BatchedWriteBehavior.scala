package com.hiya.alternator.akka.internal

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import cats.Id
import com.hiya.alternator.BatchedException.RetriesExhausted
import com.hiya.alternator.{BatchMonitoring, BatchRetryPolicy}

import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BatchedWriteBehavior[AttributeValue, BatchWriteItemResponse] extends BatchedBehavior[AttributeValue] {
  override protected type Request = BatchedWriteBehavior.WriteRequest[AttributeValue]
  override type Result = Unit
  override protected type BufferItem = BatchedWriteBehavior.WriteBuffer[AttributeValue, Ref]
  override protected type FutureResult = BatchWriteItemResponse

  private class WriteBehavior(
    client: BatchedWriteBehavior.AwsClientAdapter[AttributeValue, BatchWriteItemResponse],
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy,
    monitoring: BatchMonitoring[Id, PK]
  )(
    ctx: ActorContext[BatchedRequest],
    scheduler: TimerScheduler[BatchedRequest]
  ) extends BaseBehavior(ctx, scheduler, maxWait, retryPolicy, monitoring, 25) {

    override protected def isRetryable(ex: Throwable): Boolean = client.isRetryable(ex)
    override protected def isThrottle(ex: Throwable): Boolean = client.isThrottle(ex)

    protected override def sendSuccess(
      futureResult: BatchWriteItemResponse,
      keys: List[PK],
      buffer: Buffer
    ): (List[PK], List[PK], Buffer) = {
      val (success, failed) = client.processResult(keys, futureResult)

      val (buffer2, reschedule) = success.foldLeft(buffer -> List.empty[PK]) { case ((buffer, reschedule), key) =>
        val (refs2, bufferItem) = buffer(key).queue.dequeue
        sendResult(refs2._2, Success(()))

        if (bufferItem.isEmpty) (buffer - key) -> reschedule
        else buffer.updated(key, BatchedWriteBehavior.WriteBuffer(bufferItem, 0)) -> (key :: reschedule)
      }

      (failed, reschedule, buffer2)
    }

    protected override def sendRetriesExhausted(cause: Throwable, buffer: Buffer, pk: PK, item: BufferItem): Buffer = {
      val (refs, bufferItem) = item.queue.dequeue
      sendResult(refs._2, Failure(RetriesExhausted(cause)))

      if (bufferItem.isEmpty) buffer - pk
      else buffer.updated(pk, BatchedWriteBehavior.WriteBuffer(bufferItem, 0))
    }

    protected override def sendFailure(keys: List[PK], buffer: Buffer, ex: Throwable): Buffer = {
      keys.foldLeft(buffer) { case (buffer, key) =>
        val (refs2, bufferItem) = buffer(key).queue.dequeue
        sendResult(refs2._2, Failure(ex))

        if (bufferItem.isEmpty) buffer - key
        else buffer.updated(key, BatchedWriteBehavior.WriteBuffer(bufferItem, 0))
      }
    }

    override protected def startJob(
      keys: List[PK],
      buffer: Buffer
    ): (Future[BatchWriteItemResponse], List[PK], Buffer) = {
      // Collapse buffer: keep only the last value to write and all actorRefs
      val (buffer2, writes) = keys.foldLeft(buffer -> List.empty[(PK, Option[AV])]) { case ((buffer, writes), key) =>
        val values = buffer(key)
        val refs = values.queue.flatMap(_._2).toList
        val item = values.queue.last._1
        val buffer2 = buffer.updated(key, BatchedWriteBehavior.WriteBuffer(Queue(item -> refs), values.retries))
        buffer2 -> ((key -> item) :: writes)
      }

      (client.createQuery(writes), keys, buffer2)
    }

    override protected def receive(req: Request, ref: Ref, buffer: Buffer): (List[PK], Buffer) = {
      val key = req.pk

      buffer.get(key) match {
        case Some(elem) =>
          Nil -> buffer.updated(key, elem.copy(queue = elem.queue.enqueue(req.value -> List(ref))))

        case None =>
          List(key) -> buffer.updated(key, BatchedWriteBehavior.WriteBuffer(Queue(req.value -> List(ref)), 0))
      }
    }
  }

  protected def apply(
    client: BatchedWriteBehavior.AwsClientAdapter[AV, BatchWriteItemResponse],
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy,
    monitoring: BatchMonitoring[Id, PK]
  ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new WriteBehavior(client, maxWait, retryPolicy, monitoring)(ctx, scheduler).behavior(
          Queue.empty,
          Map.empty,
          None
        )
      }
    }

}

object BatchedWriteBehavior {
  final case class WriteRequest[AV](pk: (String, AV), value: Option[AV])

  protected final case class WriteBuffer[AV, Ref](queue: Queue[(Option[AV], List[Ref])], retries: Int)
    extends BatchedBehavior.BufferItemBase[WriteBuffer[AV, Ref]] {
    override def withRetries(retries: Int): WriteBuffer[AV, Ref] = copy(retries = retries)
  }

  trait AwsClientAdapter[AV, BatchWriteItemResponse] {
    type PK = (String, AV)

    def processResult(keys: List[PK], response: BatchWriteItemResponse): (List[PK], List[PK])
    def createQuery(key: List[(PK, Option[AV])]): Future[BatchWriteItemResponse]
    def isRetryable(ex: Throwable): Boolean
    def isThrottle(ex: Throwable): Boolean
  }

  val DEFAULT_MAX_WAIT: FiniteDuration = 5.millis
  val DEFAULT_RETRY_POLICY: BatchRetryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy()
  val DEFAULT_MONITORING: BatchMonitoring[Id, Any] = new BatchMonitoring.Disabled[Id]
}
