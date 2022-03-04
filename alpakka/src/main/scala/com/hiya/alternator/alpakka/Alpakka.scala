package com.hiya.alternator.alpakka

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import com.hiya.alternator.alpakka.AlpakkaTable.{ReadRequest, WriteRequest}
import com.hiya.alternator.alpakka.internal.{AlpakkaTableInternal, AlpakkaTableWithRangeInternal}
import com.hiya.alternator.{Client, Table, TableWithRangeKey}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.ExecutionContext

class Alpakka(client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider) extends Client {
  override type PKClient[V, PK] = AlpakkaTable[V, PK]
  override type RKClient[V, PK, RK] = AlpakkaTableWithRange[V, PK, RK]

  override def createPkClient[V, PK](table: Table[V, PK]): AlpakkaTable[V, PK] =
    new AlpakkaTableInternal(table)(client, system)

  override def createRkClient[V, PK, RK](table: TableWithRangeKey[V, PK, RK]): AlpakkaTableWithRange[V, PK, RK] =
    new AlpakkaTableWithRangeInternal(table)(client, system)
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
