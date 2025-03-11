package com.hiya.alternator.akka

import akka.Done
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.util.Timeout
import cats.Id
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator._
import com.hiya.alternator.akka.AkkaAws1.async
import com.hiya.alternator.akka.internal.BatchedWriteBehavior
import com.hiya.alternator.aws1.internal.Exceptions
import com.hiya.alternator.aws1.{Aws1DynamoDBClient, _}

import java.util.{Map => JMap}
import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** DynamoDB batched writer
  *
  * The actor waits for the maximum size of write requests (25) or maxWait time before created a batched write request.
  * If the requests fails with a retryable error the elements will be rescheduled later (using the given retryPolicy).
  * Unprocessed items are rescheduled similarly.
  *
  * The received requests are deduplicated, only the last write to the key is executed.
  */
class AkkaAws1WriteScheduler(actorRef: ActorRef[AkkaAws1WriteScheduler.BatchedRequest])(implicit scheduler: Scheduler)
  extends WriteScheduler[Future] {
  import JdkCompat.parasitic

  override def put[V, PK](table: Table[DynamoDBClient.Missing, V, PK], value: V)(implicit
    timeout: BatchTimeout
  ): Future[Unit] = {
    val key = table.schema.extract(value)
    val pk = table.schema.serializePK[AttributeValue](key)
    val av = table.schema.serializeValue.writeFields(value)

    actorRef
      .ask((ref: akka.AkkaAws1WriteScheduler.Ref) =>
        AkkaAws1WriteScheduler.Req(BatchedWriteBehavior.WriteRequest(table.tableName -> pk, Some(av)), ref)
      )(timeout.timeout, scheduler)
      .flatMap(result => Future.fromTry { result })
  }

  override def delete[V, PK](table: Table[DynamoDBClient.Missing, V, PK], key: PK)(implicit
    timeout: BatchTimeout
  ): Future[Unit] = {
    val pk = table.schema.serializePK[AttributeValue](key)

    actorRef
      .ask((ref: akka.AkkaAws1WriteScheduler.Ref) =>
        AkkaAws1WriteScheduler.Req(BatchedWriteBehavior.WriteRequest(table.tableName -> pk, None), ref)
      )(timeout.timeout, scheduler)
      .flatMap(result => Future.fromTry { result })
  }

  def terminate(timeout: FiniteDuration): Future[Done] = AkkaAws1WriteScheduler.terminate(actorRef)(timeout, scheduler)
}

object AkkaAws1WriteScheduler extends BatchedWriteBehavior[JMap[String, AttributeValue], BatchWriteItemResult] {

  private class AwsClientAdapter(client: AmazonDynamoDBAsync)
    extends Exceptions
    with BatchedWriteBehavior.AwsClientAdapter[JMap[String, AttributeValue], BatchWriteItemResult] {

    private def isSubMapOf(small: JMap[String, AttributeValue], in: JMap[String, AttributeValue]): Boolean =
      in.entrySet().containsAll(small.entrySet())

    override def processResult(keys: List[PK], response: BatchWriteItemResult): (List[PK], List[PK]) = {
      val allKeys = mutable.HashSet.from(keys)

      val unprocessedKeys = response.getUnprocessedItems.asScala.toList.flatMap { case (table, operations) =>
        val ops = operations.asScala

        val puts: mutable.Seq[PK] = ops
          .flatMap { op =>
            Option(op.getPutRequest)
              .map { item => allKeys.find { case (t, key) => t == table && isSubMapOf(key, item.getItem) }.get }
          }

        val deletes: mutable.Seq[PK] = ops
          .flatMap { op =>
            Option(op.getDeleteRequest)
              .map { table -> _.getKey }
          }

        puts ++ deletes
      }

      allKeys --= unprocessedKeys

      allKeys.toList -> unprocessedKeys
    }

    override def createQuery(key: List[(PK, Option[AV])]): Future[BatchWriteItemResult] = {
      val request = new BatchWriteItemRequest(
        key
          .groupMap(_._1._1)(x => x._1._2 -> x._2)
          .view
          .mapValues(
            _.map({
              case (_, Some(value)) => new WriteRequest(new PutRequest(value))
              case (key, None) => new WriteRequest(new DeleteRequest(key))
            }).asJava
          )
          .toMap
          .asJava
      )

      async(client.batchWriteItemAsync(request, _: AsyncHandler[BatchWriteItemRequest, BatchWriteItemResult]))
    }
  }

  def behavior(
    client: Aws1DynamoDBClient,
    maxWait: FiniteDuration = BatchedWriteBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedWriteBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedWriteBehavior.DEFAULT_MONITORING
  ): Behavior[BatchedRequest] = {
    apply(
      new AwsClientAdapter(client.underlying),
      maxWait = maxWait,
      retryPolicy = retryPolicy,
      monitoring = monitoring
    )
  }

  def apply(read: ActorRef[BatchedRequest])(implicit scheduler: Scheduler): AkkaAws1WriteScheduler = {
    new AkkaAws1WriteScheduler(read)
  }

  def apply(
    name: String,
    client: Aws1DynamoDBClient,
    shutdownTimeout: FiniteDuration = 60.seconds,
    maxWait: FiniteDuration = BatchedWriteBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedWriteBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring[Id, PK] = BatchedWriteBehavior.DEFAULT_MONITORING
  )(implicit system: ActorSystem): AkkaAws1WriteScheduler = {
    implicit val scheduler: Scheduler = system.scheduler.toTyped
    val ret = apply(system.spawn(behavior(client, maxWait, retryPolicy, monitoring), name))

    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, s"shutdown $name") { () =>
      ret.terminate(shutdownTimeout)
    }

    ret
  }

  def terminate(actorRef: ActorRef[BatchedRequest])(implicit timeout: Timeout, scheduler: Scheduler): Future[Done] = {
    actorRef.ask(GracefulShutdown)
  }
}
