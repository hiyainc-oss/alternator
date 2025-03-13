package com.hiya.alternator.akka

import akka.Done
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.util.Timeout
import cats.Id
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator._
import com.hiya.alternator.akka.internal.BatchedReadBehavior
import com.hiya.alternator.aws1.internal.Exceptions
import com.hiya.alternator.aws1._
import com.hiya.alternator.schema.DynamoFormat.Result

import java.util.{Map => JMap}
import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** DynamoDB batched reader
  *
  * The actor waits for the maximum size of read requests (100) or maxWait time before created a batched get request. If
  * the requests fails with a retryable error the elements will be rescheduled later (using the given retryPolicy).
  * Unprocessed items are rescheduled similarly.
  *
  * The received requests are deduplicated.
  */
class AkkaAws1ReadScheduler(actorRef: ActorRef[AkkaAws1ReadScheduler.BatchedRequest])(implicit scheduler: Scheduler)
  extends ReadScheduler[Future] {
  import JdkCompat.parasitic

  override def get[V, PK](table: Table[DynamoDBClient.Missing, V, PK], key: PK)(implicit
    timeout: BatchTimeout
  ): Future[Option[Result[V]]] =
    actorRef
      .ask((ref: AkkaAws1ReadScheduler.Ref) =>
        AkkaAws1ReadScheduler.Req(table.tableName -> table.schema.serializePK[AttributeValue](key), ref)
      )(timeout.timeout, scheduler)
      .flatMap(result => Future.fromTry(result.map(_.map(Aws1TableOps(table).deserialize))))

  def terminate(timeout: FiniteDuration): Future[Done] = AkkaAws1ReadScheduler.terminate(actorRef)(timeout, scheduler)
}

object AkkaAws1ReadScheduler extends BatchedReadBehavior[JMap[String, AttributeValue], BatchGetItemResult] {
  import AkkaAws1.async

  private class AwsClientAdapter(client: Aws1DynamoDBClient, overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder])
    extends Exceptions
    with BatchedReadBehavior.AwsClientAdapter[JMap[String, AttributeValue], BatchGetItemResult] {
    private def isSubMapOf(small: JMap[String, AttributeValue], in: JMap[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())

    override def createQuery(key: List[PK]): Future[BatchGetItemResult] = {
      val request = new BatchGetItemRequest(
        key.groupMap(_._1)(_._2).view.mapValues(x => new KeysAndAttributes().withKeys(x.asJava)).toMap.asJava
      )

      val requestWithOverrides = overrides.apply(request).asInstanceOf[BatchGetItemRequest]
      async(
        client.underlying
          .batchGetItemAsync(requestWithOverrides, _: AsyncHandler[BatchGetItemRequest, BatchGetItemResult])
      )
    }

    override def processResult(keys: List[PK], response: BatchGetItemResult): (List[(PK, Option[AV])], List[PK]) = {
      val allKeys = mutable.HashSet.from(keys)
      val success = List.newBuilder[(PK, Option[AV])]

      val unprocessedKeys = response.getUnprocessedKeys.asScala.toList.flatMap { case (table, keys) =>
        keys.getKeys.asScala.map(table -> _)
      }

      allKeys --= unprocessedKeys

      response.getResponses.forEach { case (table, values) =>
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

  def behavior(
    client: Aws1DynamoDBClient,
    maxWait: FiniteDuration = BatchedReadBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedReadBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedReadBehavior.DEFAULT_MONITORING,
    overrides: DynamoDBOverride[Aws1DynamoDBClient] = DynamoDBOverride.Empty
  ): Behavior[BatchedRequest] = {
    apply(
      client = new AwsClientAdapter(
        client,
        overrides = overrides(client)
      ),
      maxWait = maxWait,
      retryPolicy = retryPolicy,
      monitoring = monitoring
    )
  }

  def apply(read: ActorRef[BatchedRequest])(implicit scheduler: Scheduler): AkkaAws1ReadScheduler = {
    new AkkaAws1ReadScheduler(read)
  }

  def apply(
    name: String,
    client: Aws1DynamoDBClient,
    shutdownTimeout: FiniteDuration = 60.seconds,
    maxWait: FiniteDuration = BatchedReadBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedReadBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedReadBehavior.DEFAULT_MONITORING,
    overrides: DynamoDBOverride[Aws1DynamoDBClient] = DynamoDBOverride.Empty
  )(implicit system: ActorSystem): AkkaAws1ReadScheduler = {
    implicit val scheduler: Scheduler = system.scheduler.toTyped
    val ret = apply(system.spawn(behavior(client, maxWait, retryPolicy, monitoring, overrides), name))

    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, s"shutdown $name") { () =>
      ret.terminate(shutdownTimeout)
    }

    ret
  }

  def terminate(actorRef: ActorRef[BatchedRequest])(implicit timeout: Timeout, scheduler: Scheduler): Future[Done] = {
    actorRef.ask(GracefulShutdown)
  }
}
