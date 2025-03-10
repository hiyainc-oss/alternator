package com.hiya.alternator.akka

import _root_.akka.NotUsed
import _root_.akka.actor.ActorSystem
import _root_.akka.actor.typed.scaladsl.adapter._
import _root_.akka.stream.scaladsl.Source
import akka.testkit.TestKit
import cats.MonadThrow
import com.amazonaws.services.dynamodbv2.model
import com.hiya.alternator._
import com.hiya.alternator.aws1._
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.util.{DataPK, DataRK}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, Inside, Inspectors}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.{ClassTag, classTag}
import com.hiya.alternator.aws1.Aws1DynamoDBClient

class AkkaAws1ReadTests
  extends TestKit(ActorSystem())
  with AnyFunSpecLike
  with should.Matchers
  with Inside
  with Inspectors
  with BeforeAndAfterAll
  with BatchedRead[Aws1DynamoDBClient, Future, Source[*, NotUsed]] {
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
  override protected val stableClient: Aws1DynamoDBClient = LocalDynamoDB.client()
  override protected val lossyClient: Aws1DynamoDBClient = stableClient // new DynamoDBLossyClient(stableClient)
  override protected implicit val readScheduler: ReadScheduler[Future] =
    AkkaAws1ReadScheduler("reader", lossyClient, monitoring = monitoring, retryPolicy = retryPolicy)
  override protected implicit val DB: DynamoDB.Aux[Future, Source[*, NotUsed], Aws1DynamoDBClient] = AkkaAws1()
  override protected def eval[T](f: => Future[T]): T = Await.result(f, 10.seconds)

  override type ResourceNotFoundException = model.ResourceNotFoundException
  override def resourceNotFoundException: ClassTag[model.ResourceNotFoundException] =
    classTag[model.ResourceNotFoundException]

  describe("stream with PK table") {
    it should behave like streamRead[DataPK, String]()
  }

  describe("stream with RK table") {
    it should behave like streamRead[DataRK, (String, String)]()(DataRK.config)
  }

  it("should stop") {
    val reader = system.spawn(AkkaAws1ReadScheduler.behavior(stableClient), "reader2")
    Await.result(AkkaAws1ReadScheduler.terminate(reader)(10.seconds, system.scheduler.toTyped), 10.seconds)
  }
}
