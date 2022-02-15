package com.hiya.alternator.internal

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.hiya.alternator.{BatchRetryPolicy, Unprocessed}
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, ProvisionedThroughputExceededException}

import java.util
import java.util.concurrent.CompletionException
import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}


/**
  * States:
  * - queued: key is in the queue and in the buffer
  * - pending: key is only in the buffer
  *   - retry: there is a scheduled event to Reschedule for that key
  *   - in-progress: there is a pending aws request for that key and ClientResult or ClientFailure will be called
  */
private [alternator] trait BatchedBehavior {
  type Result
  type Ref = ActorRef[Try[Result]]
  import BatchedBehavior.PK

  protected type Request
  protected type BufferItem <: BatchedBehavior.BufferItemBase[BufferItem]
  protected type FutureResult


  sealed trait BatchedRequest
  case class Req(req: Request, ref: Ref) extends BatchedRequest
  private case object StartJob extends BatchedRequest
  private case class ClientResult(futureResult: FutureResult, pt: List[PK]) extends BatchedRequest
  private case class ClientFailure(ex: Throwable, pt: List[PK]) extends BatchedRequest
  private case class Reschedule(keys: List[PK]) extends BatchedRequest

  protected type Buffer = Map[PK, BufferItem]

  private object TimerKey

  @tailrec
  private def deque[T](q: Queue[T], n: Int, buffer: List[T] = List.empty): (List[T], Queue[T]) = {
    if(n == 0) buffer -> q
    else {
      q.dequeueOption match {
        case Some((elem, q)) => deque(q, n-1, elem::buffer)
        case None            => buffer -> q
      }
    }
  }

  abstract class BaseBehavior(
    ctx: ActorContext[BatchedRequest],
    timer: TimerScheduler[BatchedRequest],
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy,
    maxQueued: Int
  ) {
    protected def startJob(keys: List[PK], buffer: Buffer): (Future[FutureResult], List[PK], Buffer)
    protected def receive(req: Request, ref: Ref, pending: Buffer): (List[PK], Buffer)
    protected def sendFailure(keys: List[PK], buffer: Buffer, ex: Throwable): Buffer
    protected def sendRetriesExhausted(cause: Exception, buffer: Buffer, pk: PK, item: BufferItem): Buffer
    protected def sendSuccess(futureResult: FutureResult, keys: List[PK], buffer: Buffer): (List[PK], List[PK], Buffer)

    protected def sendResult(refs: List[Ref], result: Try[Result]): Unit = {
      refs.foreach { _.tell(result) }
    }

    private final def schedule(queued: Queue[PK]): Queue[PK] = {
      if (queued.length > maxQueued) {
        ctx.self.tell(StartJob)
      } else if (queued.nonEmpty && !timer.isTimerActive(TimerKey)) {
        timer.startSingleTimer(TimerKey, StartJob, maxWait)
      }
      queued
    }

    private final def handleFuture(future: Future[FutureResult], pt: List[PK]): Unit = {
      ctx.pipeToSelf(future) {
        case Success(result) => ClientResult(result, pt)
        case Failure(ex) => ClientFailure(ex, pt)
      }
    }

    private final def enqueue(queued: Queue[PK], keys: List[PK]): Queue[PK] = {
      schedule(queued ++ keys)
    }

    private def handleRetries(
      queue: Queue[PK],
      delayForThrottle: Int => Option[FiniteDuration],
      failed: List[PK],
      buffer: Buffer,
      cause: Exception,
      reschedule: List[PK] = Nil
    ): Behavior[BatchedRequest] = {
      val retryMap = mutable.TreeMap[Int, Option[FiniteDuration]]()
      val retries = mutable.TreeMap[FiniteDuration, List[PK]]()

      val newBuffer = failed.foldLeft(buffer) { case (buffer, pk) =>
        val bufferItem = buffer(pk)
        val r = bufferItem.retries

        retryMap.getOrElseUpdate(r, delayForThrottle(r)) match {
          case Some(delayTime) =>
            retries.update(delayTime, pk :: retries.getOrElse(delayTime, Nil))
            buffer.updated(pk, bufferItem.withRetries(retries = r + 1))
          case None =>
            sendRetriesExhausted(cause, buffer, pk, bufferItem)
        }
      }

      retries.toList.foreach { case (delayTime, keys) =>
        timer.startSingleTimer(Reschedule(keys), delayTime)
      }

      behavior(enqueue(queue, reschedule), newBuffer)
    }

    @tailrec
    private final def jobFailure(queue: Queue[PK], ex: Throwable, keys: List[PK], buffer: Buffer): Behavior[BatchedRequest] = {
      ex match {
        case ex : CompletionException =>
          jobFailure(queue, ex.getCause, keys, buffer)
        case ex : ProvisionedThroughputExceededException =>
          handleRetries(queue, retryPolicy.delayForThrottle, keys, buffer, ex)
        case ex : SdkServiceException if ex.isThrottlingException =>
          handleRetries(queue, retryPolicy.delayForThrottle, keys, buffer, ex)
        case ex : SdkServiceException if ex.retryable() || ex.statusCode >= 500 =>
          handleRetries(queue, retryPolicy.delayForError, keys, buffer, ex)
        case _ =>
          behavior(queue, sendFailure(keys, buffer, ex))

      }
    }

    private final def jobSuccess(queue: Queue[PK], futureResult: FutureResult, keys: List[PK], buffer: Buffer): Behavior[BatchedRequest] = {
      val (failed, reschedule, buffer2) = sendSuccess(futureResult, keys, buffer)

      handleRetries(
        queue,
        retryPolicy.delayForUnprocessed,
        failed,
        buffer2,
        Unprocessed,
        reschedule
      )
    }


    final def behavior(
      queue: Queue[PK],
      buffer: Buffer
    ): Behavior[BatchedRequest] =
      Behaviors.receiveMessage {
        case Req(req, ref) =>
          val (keys, pending2) = receive(req, ref, buffer)
          behavior(enqueue(queue, keys), pending2)

        case StartJob =>
          val (keys, queued2) = deque(queue, maxQueued)

          if (keys.isEmpty) {
            behavior(queue, buffer)
          } else {
            val (future, pt, buffer2) = startJob(keys, buffer)
            handleFuture(future, pt)
            behavior(schedule(queued2), buffer2)
          }

        case ClientResult(futureResult, pt) =>
          jobSuccess(queue, futureResult, pt, buffer)

        case ClientFailure(ex, pt) =>
          jobFailure(queue, ex, pt, buffer)

        case Reschedule(keys) =>
          ctx.self ! StartJob
          behavior(keys ++: queue, buffer)
      }

  }
}

object BatchedBehavior {
  private [alternator] type AV = util.Map[String, AttributeValue]
  private [alternator] type PK = (String, AV)

  trait BufferItemBase[T <: BufferItemBase[T]] {
    this: T =>

    def retries: Int
    def withRetries(retries: Int): T
  }
}
