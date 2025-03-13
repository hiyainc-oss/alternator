package com.hiya.alternator.akka

import akka.Done
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.util.Timeout
import cats.Id
import com.hiya.alternator._
import com.hiya.alternator.akka.internal.BatchedWriteBehavior
import com.hiya.alternator.aws2._
import com.hiya.alternator.aws2.internal.Exceptions
import software.amazon.awssdk.services.dynamodb.model._

import java.util.{Map => JMap}
import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration

/** DynamoDB batched writer
  *
  * The actor waits for the maximum size of write requests (25) or maxWait time before created a batched write request.
  * If the requests fails with a retryable error the elements will be rescheduled later (using the given retryPolicy).
  * Unprocessed items are rescheduled similarly.
  *
  * The received requests are deduplicated, only the last write to the key is executed.
  */
class AkkaAws2WriteScheduler(val actorRef: ActorRef[AkkaAws2WriteScheduler.BatchedRequest])(implicit
  scheduler: Scheduler
) extends WriteScheduler[Future] {
  import JdkCompat.parasitic

  override def put[V, PK](table: Table[DynamoDBClient.Missing, V, PK], value: V)(implicit
    timeout: BatchTimeout
  ): Future[Unit] = {
    val key = table.schema.extract(value)
    val pk = table.schema.serializePK[AttributeValue](key)
    val av = table.schema.serializeValue.writeFields(value)

    actorRef
      .ask((ref: akka.AkkaAws2WriteScheduler.Ref) =>
        AkkaAws2WriteScheduler.Req(BatchedWriteBehavior.WriteRequest(table.tableName -> pk, Some(av)), ref)
      )(timeout.timeout, scheduler)
      .flatMap(result => Future.fromTry { result })
  }

  override def delete[V, PK](table: Table[DynamoDBClient.Missing, V, PK], key: PK)(implicit
    timeout: BatchTimeout
  ): Future[Unit] = {
    val pk = table.schema.serializePK[AttributeValue](key)

    actorRef
      .ask((ref: akka.AkkaAws2WriteScheduler.Ref) =>
        AkkaAws2WriteScheduler.Req(BatchedWriteBehavior.WriteRequest(table.tableName -> pk, None), ref)
      )(timeout.timeout, scheduler)
      .flatMap(result => Future.fromTry { result })
  }

  def terminate(timeout: FiniteDuration): Future[Done] = AkkaAws2WriteScheduler.terminate(actorRef)(timeout, scheduler)
}

object AkkaAws2WriteScheduler extends BatchedWriteBehavior[JMap[String, AttributeValue], BatchWriteItemResponse] {
  import JdkCompat.CompletionStage

  private class AwsClientAdapter(
    client: Aws2DynamoDBClient,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ) extends Exceptions
    with BatchedWriteBehavior.AwsClientAdapter[JMap[String, AttributeValue], BatchWriteItemResponse] {

    private def isSubMapOf(small: JMap[String, AttributeValue], in: JMap[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())

    override def processResult(keys: List[PK], response: BatchWriteItemResponse): (List[PK], List[PK]) = {
      val allKeys = mutable.HashSet.from(keys)

      val unprocessedKeys = response.unprocessedItems().asScala.toList.flatMap { case (table, operations) =>
        val ops = operations.asScala
        val puts: mutable.Seq[PK] = ops
          .flatMap(op => Option(op.putRequest()))
          .map { item => allKeys.find { case (t, key) => t == table && isSubMapOf(key, item.item()) }.get }

        val deletes: mutable.Seq[PK] = ops
          .flatMap(op => Option(op.deleteRequest()))
          .map(table -> _.key())

        puts ++ deletes
      }

      allKeys --= unprocessedKeys

      allKeys.toList -> unprocessedKeys
    }

    override def createQuery(key: List[(PK, Option[AV])]): Future[BatchWriteItemResponse] = {
      val request = BatchWriteItemRequest
        .builder()
        .requestItems(
          key
            .groupMap(_._1._1)(x => x._1._2 -> x._2)
            .view
            .mapValues(_.map({
              case (_, Some(value)) =>
                WriteRequest.builder().putRequest(PutRequest.builder().item(value).build()).build()
              case (key, None) =>
                WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build()
            }).asJavaCollection)
            .toMap
            .asJava
        )
        .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
        .build()

      client.client.batchWriteItem(request).asScala
    }
  }

  def behavior(
    client: Aws2DynamoDBClient,
    maxWait: FiniteDuration = BatchedWriteBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedWriteBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedWriteBehavior.DEFAULT_MONITORING,
    overrides: DynamoDBOverride[Aws2DynamoDBClient] = DynamoDBOverride.empty
  ): Behavior[BatchedRequest] = {
    apply(
      new AwsClientAdapter(
        client,
        overrides = overrides(client)
      ),
      maxWait = maxWait,
      retryPolicy = retryPolicy,
      monitoring = monitoring
    )
  }

  def apply(read: ActorRef[BatchedRequest])(implicit scheduler: Scheduler): AkkaAws2WriteScheduler = {
    new AkkaAws2WriteScheduler(read)
  }

  def apply(
    name: String,
    client: Aws2DynamoDBClient,
    shutdownTimeout: FiniteDuration = 60.seconds,
    maxWait: FiniteDuration = BatchedWriteBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedWriteBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedWriteBehavior.DEFAULT_MONITORING,
    overrides: DynamoDBOverride[Aws2DynamoDBClient] = DynamoDBOverride.empty
  )(implicit system: ActorSystem): AkkaAws2WriteScheduler = {
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
