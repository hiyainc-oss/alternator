package com.hiya.alternator.akka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import cats.MonadThrow
import com.hiya.alternator._
import com.hiya.alternator.aws2.testkit.DynamoDBLossyClient
import com.hiya.alternator.aws2.{Aws2DynamoDBClient, _}
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.util.{DataPK, DataRK}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors}
import software.amazon.awssdk.services.dynamodb.model

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.{ClassTag, classTag}

class AkkaAws2WriteTests
  extends TestKit(ActorSystem())
  with AnyFunSpecLike
  with should.Matchers
  with Inside
  with Inspectors
  with BeforeAndAfterAll
  with BatchedWrite[Aws2DynamoDBClient, Future, Source[*, NotUsed]] {
  import system.dispatcher

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val retryPolicy = BatchRetryPolicy.DefaultBatchRetryPolicy(
    awsHasRetry = false,
    maxRetries = 100,
    throttleBackoff = BackoffStrategy.FullJitter(1.second, 20.millis)
  )

  override protected implicit val F: MonadThrow[Future] = _root_.cats.instances.future.catsStdInstancesForFuture
  override protected val stableClient: Aws2DynamoDBClient = LocalDynamoDB.client()
  override protected val lossyClient: Aws2DynamoDBClient = Aws2DynamoDBClient(
    new DynamoDBLossyClient(stableClient.underlying)
  )
  override protected implicit val writeScheduler: WriteScheduler[Future] =
    AkkaAws2WriteScheduler("writer", lossyClient, monitoring = monitoring, retryPolicy = retryPolicy)
  override protected implicit val DB: DynamoDB.Aux[Future, Source[*, NotUsed], Aws2DynamoDBClient] = AkkaAws2()
  override protected def eval[T](f: => Future[T]): T = Await.result(f, 10.seconds)

  override type ResourceNotFoundException = model.ResourceNotFoundException
  override def resourceNotFoundException: ClassTag[model.ResourceNotFoundException] =
    classTag[model.ResourceNotFoundException]

  describe("stream with PK table") {
    it should behave like streamWrite[DataPK, String]()(DataPK.config)
  }

  describe("stream with RK table") {
    it should behave like streamWrite[DataRK, (String, String)]()(DataRK.config)
  }

  it("should stop") {
    val reader = system.spawn(AkkaAws2WriteScheduler.behavior(stableClient), "writer2")
    Await.result(AkkaAws2WriteScheduler.terminate(reader)(10.seconds, system.scheduler.toTyped), 10.seconds)
  }
}
