package com.hiya.alternator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.stream.alpakka.dynamodb.DynamoDbOp
import com.hiya.alternator.Table.{AV, PK}
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
    extends BufferItemBase[ReadBuffer] {
    override def withRetries(retries: Int): ReadBuffer = copy(retries = retries)
  }

  override protected type Request = PK
  override protected type BufferItem = ReadBuffer
  override protected type FutureResult = BatchGetItemResponse
  override type Result = Option[AV]

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
    retryPolicy: BatchRetryPolicy,
    monitoring: BatchMonitoring
  )(
    ctx: ActorContext[BatchedRequest],
    scheduler: TimerScheduler[BatchedRequest]
  ) extends BaseBehavior(ctx, scheduler, maxWait, retryPolicy, monitoring, 100) {

    protected override def sendSuccess(futureResult: BatchGetItemResponse, keys: List[PK], buffer: Buffer): (List[PK], List[PK], Buffer) = {
      val (success, failed) = client.processResult(keys, futureResult)

      val buffer2 = success.foldLeft(buffer) { case (buffer, (key, result)) =>
        val refs = buffer(key)
        sendResult(refs.refs, Success(result))
        buffer - key
      }

      (failed, Nil, buffer2)
    }

    override protected def sendRetriesExhausted(cause: Exception, buffer: Buffer, pk: PK, item: ReadBuffer): Buffer = {
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
    backoffStrategy: BatchRetryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy(),
    monitoring: BatchMonitoring = BatchMonitoring.Disabled
   ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new ReadBehavior(new AwsClientAdapter(client), maxWait, backoffStrategy, monitoring)(ctx, scheduler).behavior(Queue.empty, Map.empty, None)
      }
    }
}

