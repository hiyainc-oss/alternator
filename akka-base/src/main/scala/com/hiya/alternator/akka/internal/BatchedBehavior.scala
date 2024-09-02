package com.hiya.alternator.akka.internal

import akka.Done
import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorRef, Behavior}
import cats.Id
import com.hiya.alternator.BatchedException.Unprocessed
import com.hiya.alternator.{BatchMonitoring, BatchRetryPolicy, SchedulerMetrics}

import java.util.concurrent.CompletionException
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/** States:
  *   - queued: key is in the queue and in the buffer
  *   - pending: key is only in the buffer
  *     - retry: there is a scheduled event to Reschedule for that key
  *     - in-progress: there is a pending aws request for that key and ClientResult or ClientFailure will be called
  */
private[alternator] abstract class BatchedBehavior[AttributeValue] {
  protected type Result
  protected type Request
  protected type BufferItem <: BatchedBehavior.BufferItemBase[BufferItem]
  protected type FutureResult

  final type AV = AttributeValue
  final type PK = (String, AV)
  final type Ref = ActorRef[Try[Result]]

  sealed trait BatchedRequest
  case class Req(req: Request, ref: Ref) extends BatchedRequest
  case class GracefulShutdown(ref: ActorRef[Done]) extends BatchedRequest
  private case object StartJob extends BatchedRequest
  private case class ClientResult(futureResult: FutureResult, keys: List[PK], durationNano: Long) extends BatchedRequest
  private case class ClientFailure(ex: Throwable, keys: List[PK], durationNano: Long) extends BatchedRequest
  private case class Reschedule(keys: List[PK]) extends BatchedRequest

  protected type Buffer = Map[PK, BufferItem]

  private object TimerKey

  @tailrec
  private def deque[T](q: Queue[T], n: Int, buffer: List[T] = List.empty): (List[T], Queue[T]) = {
    if (n == 0) buffer -> q
    else {
      q.dequeueOption match {
        case Some((elem, q)) => deque(q, n - 1, elem :: buffer)
        case None => buffer -> q
      }
    }
  }

  abstract class BaseBehavior(
    ctx: ActorContext[BatchedRequest],
    timer: TimerScheduler[BatchedRequest],
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy,
    monitoring: BatchMonitoring[Id, PK],
    maxQueued: Int
  ) extends SchedulerMetrics[Id] {
    private val name = ctx.self.path.name
    private val atomicQueueSize = new AtomicInteger()
    private val atomicInflightCount = new AtomicInteger()

    monitoring.register(name, this)

    override def queueSize: Int = atomicQueueSize.get()
    override def inflight: Int = atomicInflightCount.get()

    protected def startJob(keys: List[PK], buffer: Buffer): (Future[FutureResult], List[PK], Buffer)
    protected def receive(req: Request, ref: Ref, pending: Buffer): (List[PK], Buffer)
    protected def sendFailure(keys: List[PK], buffer: Buffer, ex: Throwable): Buffer
    protected def sendRetriesExhausted(cause: Throwable, buffer: Buffer, pk: PK, item: BufferItem): Buffer
    protected def sendSuccess(futureResult: FutureResult, keys: List[PK], buffer: Buffer): (List[PK], List[PK], Buffer)

    protected def sendResult(refs: List[Ref], result: Try[Result]): Unit = {
      atomicQueueSize.addAndGet(-refs.size)
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

    private final def handleFuture(future: Future[FutureResult], keys: List[PK], startNano: Long): Unit = {
      ctx.pipeToSelf(future) {
        case Success(result) => ClientResult(result, keys, System.nanoTime() - startNano)
        case Failure(ex) => ClientFailure(ex, keys, System.nanoTime() - startNano)
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
      cause: Throwable,
      shutdown: Option[ActorRef[Done]],
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

      monitoring.retries(name, failed)

      retries.toList.foreach { case (delayTime, keys) =>
        timer.startSingleTimer(Reschedule(keys), delayTime)
      }

      checkShutdown(enqueue(queue, reschedule), newBuffer, shutdown)
    }

    @tailrec
    private final def jobFailure(
      queue: Queue[PK],
      ex: Throwable,
      keys: List[PK],
      buffer: Buffer,
      durationNano: Long,
      shutdown: Option[ActorRef[Done]]
    ): Behavior[BatchedRequest] = {
      ex match {
        case ex: CompletionException =>
          jobFailure(queue, ex.getCause, keys, buffer, durationNano, shutdown)

        case ex if isThrottle(ex) =>
          monitoring.requestComplete(name, Some(ex), keys, durationNano)
          handleRetries(queue, retryPolicy.delayForThrottle, keys, buffer, ex, shutdown)

        case ex if isRetryable(ex) =>
          monitoring.requestComplete(name, Some(ex), keys, durationNano)
          handleRetries(queue, retryPolicy.delayForError, keys, buffer, ex, shutdown)

        case _ =>
          monitoring.requestComplete(name, Some(ex), keys, durationNano)
          checkShutdown(queue, sendFailure(keys, buffer, ex), shutdown)
      }
    }

    protected def isRetryable(ex: Throwable): Boolean
    protected def isThrottle(ex: Throwable): Boolean

    private final def jobSuccess(
      queue: Queue[PK],
      futureResult: FutureResult,
      keys: List[PK],
      buffer: Buffer,
      durationNano: Long,
      shutdown: Option[ActorRef[Done]]
    ): Behavior[BatchedRequest] = {
      monitoring.requestComplete(name, None, keys, durationNano)

      val (failed, reschedule, buffer2) = sendSuccess(futureResult, keys, buffer)

      handleRetries(
        queue,
        retryPolicy.delayForUnprocessed,
        failed,
        buffer2,
        Unprocessed,
        shutdown,
        reschedule
      )
    }

    private def checkShutdown(
      queue: Queue[PK],
      buffer: Buffer,
      running: Option[ActorRef[Done]]
    ): Behavior[BatchedRequest] = {
      running match {
        case Some(ref) if queue.isEmpty =>
          monitoring.close()
          ref.tell(Done)
          Behaviors.stopped
        case _ =>
          behavior(queue, buffer, running)
      }
    }

    final def behavior(
      queue: Queue[PK],
      buffer: Buffer,
      shutdown: Option[ActorRef[Done]]
    ): Behavior[BatchedRequest] =
      Behaviors.receiveMessage {
        case GracefulShutdown(ref) =>
          if (shutdown.isEmpty) {
            checkShutdown(queue, buffer, Some(ref))
          } else {
            Behaviors.unhandled
          }

        case Req(req, ref) =>
          if (shutdown.isEmpty) {
            atomicQueueSize.incrementAndGet()
            val (keys, pending2) = receive(req, ref, buffer)
            checkShutdown(enqueue(queue, keys), pending2, shutdown)
          } else Behaviors.unhandled

        case StartJob =>
          val (keys, queued2) = deque(queue, maxQueued)
          atomicInflightCount.addAndGet(keys.size)

          if (keys.isEmpty) {
            checkShutdown(queue, buffer, shutdown)
          } else {
            val startTime = System.nanoTime()
            val (future, pt, buffer2) = startJob(keys, buffer)
            handleFuture(future, pt, startTime)
            checkShutdown(schedule(queued2), buffer2, shutdown)
          }

        case ClientResult(futureResult, keys, durationNano) =>
          atomicInflightCount.addAndGet(-keys.size)
          jobSuccess(queue, futureResult, keys, buffer, durationNano, shutdown)

        case ClientFailure(ex, keys, durationNano) =>
          atomicInflightCount.addAndGet(-keys.size)
          jobFailure(queue, ex, keys, buffer, durationNano, shutdown)

        case Reschedule(keys) =>
          ctx.self ! StartJob
          checkShutdown(keys ++: queue, buffer, shutdown)
      }

  }
}

object BatchedBehavior {
  trait BufferItemBase[T <: BufferItemBase[T]] {
    this: T =>

    def retries: Int
    def withRetries(retries: Int): T
  }
}
