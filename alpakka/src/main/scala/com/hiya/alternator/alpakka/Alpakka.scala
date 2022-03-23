package com.hiya.alternator.alpakka

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior, Props, Scheduler}
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import com.hiya.alternator.alpakka.AlpakkaTableOps.{ReadRequest, WriteRequest}
import com.hiya.alternator.alpakka.internal.{AlpakkaTableOpsInternal, AlpakkaTableOpsWithRangeInternal}
import com.hiya.alternator.{Client, Table, TableWithRangeKey}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class Alpakka(val client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider) extends Client {
  override type PKClient[V, PK] = AlpakkaTableOps[V, PK]
  override type RKClient[V, PK, RK] = AlpakkaTableOpsWithRange[V, PK, RK]

  override def createPkClient[V, PK](table: Table[V, PK]): AlpakkaTableOps[V, PK] =
    new AlpakkaTableOpsInternal(table)(client, system)

  override def createRkClient[V, PK, RK](table: TableWithRangeKey[V, PK, RK]): AlpakkaTableOpsWithRange[V, PK, RK] =
    new AlpakkaTableOpsWithRangeInternal(table)(client, system)

  def createBatchedReader(
    name: String,
    maxWait: FiniteDuration = BatchedReadBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedReadBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring = BatchedReadBehavior.DEFAULT_MONITORING,
    props: Props = Props.empty
  ): ActorRef[BatchedReadBehavior.BatchedRequest] = {
    system.classicSystem.spawn(
      createReaderBehaviour(maxWait, retryPolicy, monitoring),
      name,
      props
    )
  }

  def createReaderBehaviour(
    maxWait: FiniteDuration = BatchedReadBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedReadBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring = BatchedReadBehavior.DEFAULT_MONITORING
  ): Behavior[BatchedReadBehavior.BatchedRequest] = {
    BatchedReadBehavior(client = client, maxWait = maxWait, retryPolicy = retryPolicy, monitoring = monitoring)
  }

  def createBatchedWriter(
     name: String,
     maxWait: FiniteDuration = BatchedWriteBehavior.DEFAULT_MAX_WAIT,
     retryPolicy: BatchRetryPolicy = BatchedWriteBehavior.DEFAULT_RETRY_POLICY,
     monitoring: BatchMonitoring = BatchedWriteBehavior.DEFAULT_MONITORING,
     props: Props = Props.empty
  ): ActorRef[BatchedWriteBehavior.BatchedRequest] = {
    system.classicSystem.spawn(
      createWriterBehaviour(maxWait, retryPolicy, monitoring),
      name,
      props
    )
  }

  def createWriterBehaviour(
    maxWait: FiniteDuration = BatchedWriteBehavior.DEFAULT_MAX_WAIT,
    retryPolicy: BatchRetryPolicy = BatchedWriteBehavior.DEFAULT_RETRY_POLICY,
    monitoring: BatchMonitoring = BatchedWriteBehavior.DEFAULT_MONITORING
  ): Behavior[BatchedWriteBehavior.BatchedRequest] = {
    BatchedWriteBehavior(client = client, maxWait = maxWait, retryPolicy = retryPolicy, monitoring = monitoring)
  }
}

object Alpakka {
  def apply(client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider): Alpakka = new Alpakka(client)

  def orderedWriter[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[WriteRequest[V], V, NotUsed] =
    Flow[WriteRequest[V]].mapAsync(parallelism)(_.send())

  def unorderedWriter[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[WriteRequest[V], V, NotUsed] =
    Flow[WriteRequest[V]].mapAsyncUnordered(parallelism)(_.send())


  private [alternator] implicit lazy val parasitic: ExecutionContext = {
    // The backport is present in akka, so we will just use it by reflection
    // It probably will not change, as it is a stable internal api
    val q = akka.dispatch.ExecutionContexts
    val clazz = q.getClass
    val field = clazz.getDeclaredField("parasitic")
    field.setAccessible(true)
    val ret = field.get(q).asInstanceOf[ExecutionContext]
    field.setAccessible(false)
    ret
  }


  def orderedReader[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[ReadRequest[V], V, NotUsed] =
    Flow[ReadRequest[V]].mapAsync(parallelism)(_.send())

  def unorderedReader[V](parallelism: Int)(implicit actorRef: ActorRef[BatchedReadBehavior.BatchedRequest], timeout: Timeout, scheduler: Scheduler): Flow[ReadRequest[V], V, NotUsed] =
    Flow[ReadRequest[V]].mapAsyncUnordered(parallelism)(_.send())

}
