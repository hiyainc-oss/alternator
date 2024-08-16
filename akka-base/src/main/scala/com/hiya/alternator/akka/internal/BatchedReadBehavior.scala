package com.hiya.alternator.akka.internal

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import cats.Id
import com.hiya.alternator.BatchedException.RetriesExhausted
import com.hiya.alternator.akka.internal.BatchedBehavior.BufferItemBase
import com.hiya.alternator.{BatchMonitoring, BatchRetryPolicy}

import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BatchedReadBehavior[AttributeValue, BatchGetItemResponse] extends BatchedBehavior[AttributeValue] {
  override protected type Request = PK
  override protected type BufferItem = BatchedReadBehavior.ReadBuffer[Ref]
  override protected type FutureResult = BatchGetItemResponse
  override protected type Result = Option[AV]

  private class ReadBehavior(
    client: BatchedReadBehavior.AwsClientAdapter[AV, FutureResult],
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy,
    monitoring: BatchMonitoring[Id, PK]
  )(
    ctx: ActorContext[BatchedRequest],
    scheduler: TimerScheduler[BatchedRequest]
  ) extends BaseBehavior(ctx, scheduler, maxWait, retryPolicy, monitoring, 100) {

    protected override def sendSuccess(
      futureResult: BatchGetItemResponse,
      keys: List[PK],
      buffer: Buffer
    ): (List[PK], List[PK], Buffer) = {
      val (success, failed) = client.processResult(keys, futureResult)

      val buffer2 = success.foldLeft(buffer) { case (buffer, (key, result)) =>
        val refs = buffer(key)
        sendResult(refs.refs, Success(result))
        buffer - key
      }

      (failed, Nil, buffer2)
    }

    override protected def isRetryable(ex: Throwable): Boolean = client.isRetryable(ex)
    override protected def isThrottle(ex: Throwable): Boolean = client.isThrottle(ex)

    override protected def sendRetriesExhausted(cause: Throwable, buffer: Buffer, pk: PK, item: BufferItem): Buffer = {
      sendResult(item.refs, Failure(RetriesExhausted(cause)))
      buffer - pk
    }

    override protected def sendFailure(keys: List[PK], buffer: Buffer, ex: Throwable): Buffer = {
      keys.foldLeft(buffer) { case (buffer, key) =>
        val refs = buffer(key)
        sendResult(refs.refs, Failure(ex))
        buffer - key
      }
    }

    override protected def startJob(
      keys: List[PK],
      buffer: Buffer
    ): (Future[BatchGetItemResponse], List[PK], Buffer) = {
      (client.createQuery(keys), keys, buffer)
    }

    override protected def receive(key: PK, ref: Ref, buffer: Buffer): (List[PK], Buffer) = {
      buffer.get(key) match {
        case Some(elem) =>
          Nil -> buffer.updated(key, elem.copy(refs = ref :: elem.refs))

        case None =>
          List(key) -> buffer.updated(key, BatchedReadBehavior.ReadBuffer(ref :: Nil, 0))
      }
    }
  }

  protected def apply(
    client: BatchedReadBehavior.AwsClientAdapter[AV, BatchGetItemResponse],
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy,
    monitoring: BatchMonitoring[Id, PK]
  ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new ReadBehavior(client, maxWait, retryPolicy, monitoring)(ctx, scheduler).behavior(
          Queue.empty,
          Map.empty,
          None
        )
      }
    }

}

object BatchedReadBehavior {
  val DEFAULT_MAX_WAIT: FiniteDuration = 5.millis
  val DEFAULT_RETRY_POLICY: BatchRetryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy()
  val DEFAULT_MONITORING: BatchMonitoring[Id, Any] = new BatchMonitoring.Disabled[Id]

  trait AwsClientAdapter[AV, FutureResult] {
    private final type PK = (String, AV)
    def processResult(keys: List[(String, AV)], futureResult: FutureResult): (List[(PK, Option[AV])], List[PK])
    def createQuery(key: List[PK]): Future[FutureResult]
    def isRetryable(ex: Throwable): Boolean
    def isThrottle(ex: Throwable): Boolean
  }

  protected final case class ReadBuffer[Ref](refs: List[Ref], retries: Int) extends BufferItemBase[ReadBuffer[Ref]] {
    override def withRetries(retries: Int): ReadBuffer[Ref] = copy(retries = retries)
  }
}
