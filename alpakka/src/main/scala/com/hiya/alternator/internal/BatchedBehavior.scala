package com.hiya.alternator.internal

import java.util

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import BatchedBehavior.RetryPolicy
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import scala.collection.compat._


private [alternator] trait BatchedBehavior {
  protected type Request
  type Result
  protected type Buffer
  protected type FutureResult
  protected type FuturePassThru

  type Ref = ActorRef[Result]
  import BatchedBehavior.PK

  sealed trait BatchedRequest
  case class Req(req: Request) extends BatchedRequest
  private case object StartJob extends BatchedRequest
  private case class ClientResult(futureResult: FutureResult, pt: FuturePassThru) extends BatchedRequest
  private case class ClientFailure(ex: Throwable, pt: FuturePassThru) extends BatchedRequest
  private case class Reschedule(keys: List[PK]) extends BatchedRequest

  protected case class ProcessResult(
    enqueue: List[PK],
    retry: List[(Int, PK)],
    buffer: Buffer,
    jobs: List[(FuturePassThru, Future[FutureResult])]
  )

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
    maxQueued: Int,
    retryPolicy: RetryPolicy
  ) {
    protected def jobSuccess(futureResult: FutureResult, pt: FuturePassThru, buffer: Buffer): ProcessResult
    protected def jobFailure(ex: Throwable, pt: FuturePassThru, buffer: Buffer): ProcessResult
    protected def startJob(keys: List[PK], buffer: Buffer): (Future[FutureResult], FuturePassThru, Buffer)
    protected def receive(req: Request, pending: Buffer): (List[PK], Buffer)

    protected def sendResult(refs: List[Ref], result: Result): Unit = {
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

    private final def handleFuture(future: Future[FutureResult], pt: FuturePassThru): Unit = {
      ctx.pipeToSelf(future) {
        case Success(result) => ClientResult(result, pt)
        case Failure(ex) => ClientFailure(ex, pt)
      }
    }

    private final def handleJobResult(queue: Queue[PK], result: ProcessResult): Behavior[BatchedRequest] = {

      result.retry.groupMap(_._1)(_._2).foreach { case (retry, keys) =>
        timer.startSingleTimer(Reschedule(keys), retryPolicy.getRetry(retry))
      }

      result.jobs.foreach { case (pt, future) =>
        handleFuture(future, pt)
      }

      behavior(enqueue(queue, result.enqueue), result.buffer)
    }

    private final def enqueue(queued: Queue[PK], keys: List[PK]): Queue[PK] = {
      schedule(queued ++ keys)
    }

    final def behavior(
      queue: Queue[PK],
      buffer: Buffer
    ): Behavior[BatchedRequest] =
      Behaviors.receiveMessage {
        case Req(req) =>
          val (keys, pending2) = receive(req, buffer)
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
          handleJobResult(queue, jobSuccess(futureResult, pt, buffer))

        case ClientFailure(ex, pt) =>
          handleJobResult(queue, jobFailure(ex, pt, buffer))

        case Reschedule(keys) =>
          ctx.self ! StartJob
          behavior(keys ++: queue, buffer)
      }

  }
}

object BatchedBehavior {
  private [alternator] type AV = util.Map[String, AttributeValue]
  private [alternator] type PK = (String, AV)

  trait RetryPolicy {
    def getRetry(retry: Int): FiniteDuration
  }
}
