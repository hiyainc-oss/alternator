package com.hiya.alternator

import java.util

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.stream.alpakka.dynamodb.DynamoDbOp
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, BatchGetItemRequest, BatchGetItemResponse, KeysAndAttributes}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._


object BatchedReadBehavior extends internal.BatchedBehavior {
  import internal.BatchedBehavior._

  private [alternator] final case class ReadBuffer(refs: List[Ref], retries: Int)
  private [alternator] final case class ReadRequest(key: PK, ref: Ref)

  override protected type Request = ReadRequest
  override protected type Result = Option[AV]
  override protected type Buffer = Map[PK, ReadBuffer]


  override protected type FutureResult = BatchGetItemResponse
  override protected type FuturePassThru = List[PK]

  private class AwsClientAdapter(client: DynamoDbAsyncClient) {
    private def isSubMapOf(small: util.Map[String, AttributeValue], in: util.Map[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())

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

    def createQuery(key: List[(String, AV)]): Future[BatchGetItemResponse] = {
      val request = BatchGetItemRequest.builder()
        .requestItems(
          key.groupMap(_._1)(_._2).view.mapValues(x => KeysAndAttributes.builder().keys(x.asJava).build()).toMap.asJava
        )
        .build()

      DynamoDbOp.batchGetItem.execute(request)(client)
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
        sendResult(refs.refs, result)
        buffer.removed(key)
      }

      val (buffer3, retries) = failed
        .foldLeft(buffer2 -> List.empty[(Int, PK)]) { case ((pending, retries), pk) =>
          val buffer = pending(pk)
          pending.updated(pk, buffer.copy(retries = buffer.retries + 1)) -> ((buffer.retries -> pk) :: retries)
        }

      ProcessResult(Nil, retries, buffer3, Nil)
    }

    override protected def jobFailure(ex: Throwable, pt: FuturePassThru, buffer: Buffer): BatchedReadBehavior.ProcessResult = ???

    override protected def startJob(keys: List[PK], buffer: Buffer): (Future[BatchGetItemResponse], List[PK], Buffer) = {
      (client.createQuery(keys), keys, buffer)
    }

    override protected def receive(req: ReadRequest, buffer: Buffer): (List[PK], Buffer) = {
      val key = req.key

      buffer.get(key) match {
        case Some(elem) =>
          Nil -> buffer.updated(key, elem.copy(refs = req.ref :: elem.refs))

        case None =>
          List(key) -> buffer.updated(key, ReadBuffer(req.ref :: Nil, 0))
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

