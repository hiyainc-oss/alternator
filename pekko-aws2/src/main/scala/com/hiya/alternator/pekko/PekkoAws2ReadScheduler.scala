package com.hiya.alternator.pekko

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Scheduler}
import org.apache.pekko.actor.{ActorSystem, CoordinatedShutdown}
import org.apache.pekko.util.Timeout
import cats.Id
import com.hiya.alternator._
import com.hiya.alternator.pekko.internal.BatchedReadBehavior
import com.hiya.alternator.aws2._
import com.hiya.alternator.aws2.internal.Exceptions
import com.hiya.alternator.schema.DynamoFormat.Result
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.model._

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
class PekkoAws2ReadScheduler(actorRef: ActorRef[PekkoAws2ReadScheduler.BatchedRequest])(implicit scheduler: Scheduler)
  extends ReadScheduler[Future] {
  import JdkCompat.parasitic

  override def get[V, PK](table: Table[DynamoDBClient.Missing, V, PK], key: PK)(implicit
    timeout: BatchTimeout
  ): Future[Option[Result[V]]] = {
    actorRef
      .ask((ref: PekkoAws2ReadScheduler.Ref) =>
        PekkoAws2ReadScheduler.Req(table.tableName -> table.schema.serializePK[AttributeValue](key), ref)
      )(timeout.timeout, scheduler)
      .flatMap(result => Future.fromTry(result.map(_.map(table.schema.serializeValue.readFields(_)))))
  }

  def terminate(timeout: FiniteDuration): Future[Done] = PekkoAws2ReadScheduler.terminate(actorRef)(timeout, scheduler)
}

object PekkoAws2ReadScheduler extends BatchedReadBehavior[JMap[String, AttributeValue], BatchGetItemResponse] {
  import JdkCompat.CompletionStage

  private class AwsClientAdapter(
    client: Aws2DynamoDBClient,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ) extends Exceptions
    with BatchedReadBehavior.AwsClientAdapter[JMap[String, AttributeValue], BatchGetItemResponse] {
    private def isSubMapOf(small: JMap[String, AttributeValue], in: JMap[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())

    override def createQuery(key: List[PK]): Future[BatchGetItemResponse] = {
      val request = BatchGetItemRequest
        .builder()
        .requestItems(
          key.groupMap(_._1)(_._2).view.mapValues(x => KeysAndAttributes.builder().keys(x.asJava).build()).toMap.asJava
        )
        .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
        .build()

      client.client.batchGetItem(request).asScala
    }
    override def processResult(keys: List[PK], response: BatchGetItemResponse): (List[(PK, Option[AV])], List[PK]) = {
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

  def behavior(
    client: Aws2DynamoDBClient,
    maxWait: FiniteDuration = BatchedReadBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedReadBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedReadBehavior.DEFAULT_MONITORING,
    overrides: DynamoDBOverride[Aws2DynamoDBClient] = DynamoDBOverride.empty
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

  def apply(read: ActorRef[BatchedRequest])(implicit scheduler: Scheduler): PekkoAws2ReadScheduler = {
    new PekkoAws2ReadScheduler(read)
  }

  def apply(
    name: String,
    client: Aws2DynamoDBClient,
    shutdownTimeout: FiniteDuration = 60.seconds,
    maxWait: FiniteDuration = BatchedReadBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedReadBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedReadBehavior.DEFAULT_MONITORING,
    overrides: DynamoDBOverride[Aws2DynamoDBClient] = DynamoDBOverride.empty
  )(implicit system: ActorSystem): PekkoAws2ReadScheduler = {
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
