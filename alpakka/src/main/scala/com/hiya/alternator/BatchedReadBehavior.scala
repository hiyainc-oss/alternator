package com.hiya.alternator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.stream.alpakka.dynamodb.DynamoDbOp
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util.{Map => JMap}
import scala.collection.compat._
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}


object BatchedReadBehavior extends internal.BatchedBehavior {
  import internal.BatchedBehavior._

  private [alternator] final case class ReadBuffer(refs: List[Ref], retries: Int)

  override protected type Request = PK
  override type Result = Option[AV]
  override protected type Buffer = Map[PK, ReadBuffer]


  override protected type FutureResult = BatchGetItemResponse
  override protected type FuturePassThru = List[PK]

  private class AwsClientAdapter(client: DynamoDbAsyncClient) {

    private def isSubMapOf(small: JMap[String, AttributeValue], in: JMap[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())


    def createQuery(key: List[PK]): Future[BatchGetItemResponse] = {
      val request = BatchGetItemRequest.builder()
        .requestItems(
          key.groupMap(_._1)(_._2).view.mapValues(x => KeysAndAttributes.builder().keys(x.asJava).build()).toMap.asJava
        )
        .build()

      DynamoDbOp.batchGetItem.execute(request)(client)
    }

    /**
     * Calculates processed and unprocessed items from the response from dynamodb.
     *
     * The result is a tuple of:
     *   - list of key-value pairs for the successful items, where the value is None if the row does not exists
     *   - list of keys for the unsuccessful items
     *
     * @param keys original keys for the request
     * @param response response from the dynamodb client
     * @return a pair of processed and unprocessed items
     */
    def processResult(keys: List[PK], response: BatchGetItemResponse): (List[(PK, Option[AV])], List[PK]) = {
      val allKeys = mutable.HashSet.from(keys)
      val success = List.newBuilder[(PK, Option[AV])]

      val unprocessedKeys = response.unprocessedKeys.asScala.toList.flatMap { case (table, keys) =>
        keys.keys().asScala.map(table -> _)
      }

      allKeys --= unprocessedKeys

      response.responses().forEach { case (table, values) =>
        values.forEach { av =>
          val key = allKeys.find { case (t, key) => t == table && isSubMapOf(key, av) }.get
          allKeys -= key
          success += key -> Some(av)
        }
      }

      allKeys.foreach { key => success += key -> None }

      success.result() -> unprocessedKeys
    }
  }

  private class ReadBehavior(
    client: AwsClientAdapter,
    maxWait: FiniteDuration,
    retryPolicy: BatchRetryPolicy
  )(
    ctx: ActorContext[BatchedRequest],
    scheduler: TimerScheduler[BatchedRequest]
  ) extends BaseBehavior(ctx, scheduler, maxWait, 100) {


    override protected def jobSuccess(futureResult: BatchGetItemResponse, keys: FuturePassThru, buffer: Buffer): BatchedReadBehavior.ProcessResult = {
      val (success, failed) = client.processResult(keys, futureResult)

      val buffer2 = success.foldLeft(buffer) { case (buffer, (key, result)) =>
        val refs = buffer(key)
        sendResult(refs.refs, Success(result))
        buffer - key
      }

      getRetries(retryPolicy.delayForUnprocessed, failed, buffer2, Unprocessed)
    }

    private def getRetries(
      delayForThrottle: Int => Option[FiniteDuration],
      failed: List[(String, AV)],
      buffer: Map[(String, AV), ReadBuffer],
      cause: Exception
    ): ProcessResult = {
      var newBuffer = buffer
      val retryMap = mutable.TreeMap[Int, Option[FiniteDuration]]()
      val retries = mutable.TreeMap[FiniteDuration, List[PK]]()

      failed.foreach { pk =>
        val buffer = newBuffer(pk)
        val r = buffer.retries
        retryMap.getOrElseUpdate(r, delayForThrottle(r)) match {
          case Some(delayTime) =>
            newBuffer = newBuffer.updated(pk, buffer.copy(retries = r + 1))
            retries.update(delayTime, pk :: retries.getOrElse(delayTime, Nil))
          case None =>
            newBuffer = newBuffer.removed(pk)
            sendResult(buffer.refs, Failure(RetriesExhausted(cause)))
        }
      }

      ProcessResult(Nil, retries.toList, newBuffer, Nil)
    }


    override protected def jobFailure(ex: Throwable, keys: FuturePassThru, buffer: Buffer): BatchedReadBehavior.ProcessResult = {
      ex match {
        case ex : ProvisionedThroughputExceededException =>
          getRetries(retryPolicy.delayForThrottle, keys, buffer, ex)
        case ex : SdkServiceException if ex.isThrottlingException  =>
          getRetries(retryPolicy.delayForThrottle, keys, buffer, ex)
        case ex : SdkServiceException if ex.retryable() || ex.statusCode >= 500 =>
          getRetries(retryPolicy.delayForError, keys, buffer, ex)
        case _ =>
          val buffer2 = keys.foldLeft(buffer) { case (buffer, key) =>
            val refs = buffer(key)
            sendResult(refs.refs, Failure(ex))
            buffer - key
          }
          ProcessResult(Nil, Nil, buffer2, Nil)
      }
    }

    override protected def startJob(keys: List[PK], buffer: Buffer): (Future[BatchGetItemResponse], List[PK], Buffer) = {
      (client.createQuery(keys), keys, buffer)
    }

    override protected def receive(key: PK, ref: Ref, buffer: Buffer): (List[PK], Buffer) = {
      buffer.get(key) match {
        case Some(elem) =>
          Nil -> buffer.updated(key, elem.copy(refs = ref :: elem.refs))

        case None =>
          List(key) -> buffer.updated(key, ReadBuffer(ref :: Nil, 0))
      }
    }
  }

  /**
   * DynamoDB batched reader
   *
   * The actor waits for the maximum size of read requests (100) or maxWait time before created a batched get
   * request. If the requests fails with a retryable error the elements will be rescheduled later (using the given
   * retryPolicy). Unprocessed items are rescheduled similarly.
   *
   * The received requests are deduplicated.
   *
   */
  def apply(
    client: DynamoDbAsyncClient,
    maxWait: FiniteDuration = 5.millis,
    backoffStrategy: BatchRetryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy()
  ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new ReadBehavior(new AwsClientAdapter(client), maxWait, backoffStrategy)(ctx, scheduler).behavior(Queue.empty, Map.empty)
      }
    }
}

