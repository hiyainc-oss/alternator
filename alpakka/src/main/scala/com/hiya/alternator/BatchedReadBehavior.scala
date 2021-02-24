package com.hiya.alternator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.stream.alpakka.dynamodb.DynamoDbOp
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, BatchGetItemRequest, BatchGetItemResponse, KeysAndAttributes}

import java.util
import scala.collection.compat._
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
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
    private def isSubMapOf(small: util.Map[String, AttributeValue], in: util.Map[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())


    /**
     *
     */
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
    retryPolicy: RetryPolicy
  )(
    ctx: ActorContext[BatchedRequest],
    scheduler: TimerScheduler[BatchedRequest]
  ) extends BaseBehavior(ctx, scheduler, maxWait, 100, retryPolicy) {


    override protected def jobSuccess(futureResult: BatchGetItemResponse, keys: FuturePassThru, buffer: Buffer): BatchedReadBehavior.ProcessResult = {
      val (success, failed) = client.processResult(keys, futureResult)

      val buffer2 = success.foldLeft(buffer) { case (buffer, (key, result)) =>
        val refs = buffer(key)
        sendResult(refs.refs, Success(result))
        buffer - key
      }

      val (buffer3: Map[(String, AV), ReadBuffer], retries: List[(Int, PK)]) = getRetries(failed, buffer2)

      ProcessResult(Nil, retries, buffer3, Nil)
    }

    private def getRetries(failed: List[(String, AV)], buffer: Map[(String, AV), ReadBuffer]): (Map[(String, AV), ReadBuffer], List[(Int, (String, AV))]) = {
      failed
        .foldLeft(buffer -> List.empty[(Int, PK)]) { case ((pending, retries), pk) =>
          val buffer = pending(pk)
          pending.updated(pk, buffer.copy(retries = buffer.retries + 1)) -> ((buffer.retries -> pk) :: retries)
        }
    }

    override protected def jobFailure(ex: Throwable, keys: FuturePassThru, buffer: Buffer): BatchedReadBehavior.ProcessResult = {
      ex match {
        case ex : SdkServiceException if ex.isThrottlingException || ex.retryable() || ex.statusCode >= 500 =>
          val (buffer2, retries) = getRetries(keys, buffer)
          ProcessResult(Nil, retries, buffer2, Nil)
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

  def apply(
    client: DynamoDbAsyncClient,
    maxWait: FiniteDuration,
    retryPolicy: RetryPolicy
  ): Behavior[BatchedRequest] =
    Behaviors.setup { ctx =>
      Behaviors.withTimers { scheduler =>
        new ReadBehavior(new AwsClientAdapter(client), maxWait, retryPolicy)(ctx, scheduler).behavior(Queue.empty, Map.empty)
      }
    }
}

