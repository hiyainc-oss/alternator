package com.hiya.alternator

import cats.syntax.all._
import cats.{Id, MonadThrow}
import com.hiya.alternator.util.TableConfig
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should
import org.scalatest.{Inside, Inspectors}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random

trait BatchedRead[ClientT, F[+_], S[_]] {
  this: AnyFunSpecLike with should.Matchers with Inspectors with Inside =>

  protected implicit def F: MonadThrow[F]
  protected implicit def readScheduler: ReadScheduler[F]
  protected def stableClient: ClientT
  protected def lossyClient: ClientT
  protected implicit def DB: DynamoDB.Aux[F, S, ClientT]
  protected def eval[T](f: => F[T]): T
  protected type ResourceNotFoundException <: Throwable
  protected def resourceNotFoundException: ClassTag[ResourceNotFoundException]

  protected implicit val timeout: BatchTimeout = BatchTimeout(10.seconds)

  protected object monitoring extends BatchMonitoring[Id, Any] {
    private var inflightF: () => Int = _
    private var queueSizeF: () => Int = _
    var retries = 0
    var requests = 0

    def inflight(): Int = inflightF()
    def queueSize(): Int = queueSizeF()

    override def register(actorName: String, behavior: SchedulerMetrics[Id]): Unit = {
      inflightF = () => behavior.inflight
      queueSizeF = () => behavior.queueSize
    }

    override def retries(actorName: String, failed: List[Any]): Unit = ()
    override def requestComplete(actorName: String, ex: Option[Throwable], keys: List[Any], durationNano: Long): Unit =
      ()
    override def close(): Unit = {}
  }

  def streamRead[Data, Key](implicit tableConfig: TableConfig[Data, Key, Table[*, Data, Key]]): Unit = {
    def writeData(table: Table[ClientT, Data, Key], nums: immutable.Iterable[Int]): F[Unit] = {
      nums
        .map(v => tableConfig.createData(v)._2)
        .toList
        .traverse(DB.put(table, _))
        .map(_ => ())
    }

    def withData[T](nums: immutable.Iterable[Int])(f: Table[ClientT, Data, Key] => F[T]): F[T] = {
      tableConfig.withTable(stableClient).eval { table =>
        writeData(table, nums) >> f(table)
      }
    }

    it("should report if table not exists") {
      val table = tableConfig.table("doesnotexists", stableClient)
      implicit val classTag: ClassTag[ResourceNotFoundException] = resourceNotFoundException
      val _ = intercept[ResourceNotFoundException] {
        eval {
          List(1)
            .map(k => tableConfig.createData(k))
            .map(_._1)
            .traverse(readScheduler.get(table.noClient, _))
        }
      }

      monitoring.inflight() shouldBe 0
      monitoring.queueSize() shouldBe 0
    }

    it("should read data") {
      val result = eval {
        withData(1 to 100) { table =>
          Random
            .shuffle(List.fill(10)(1 to 100).flatten)
            .map(k => tableConfig.createData(k))
            .traverse { case (key, value) =>
              readScheduler.get(table.noClient, key).map(_ -> value)
            }
        }
      }

      result.size shouldBe 1000
      monitoring.inflight() shouldBe 0
      monitoring.queueSize() shouldBe 0

      forAll(result) { case (data, pt) =>
        inside(data) { case Some(Right(p)) =>
          p shouldBe pt
        }
      }
    }

    it("should read empty table") {
      val result = eval {
        tableConfig.withTable(stableClient).eval { table =>
          Random
            .shuffle(List.fill(10)(1 to 100).flatten)
            .map(k => tableConfig.createData(k))
            .traverse { case (key, _) =>
              readScheduler.get(table.noClient, key)
            }
        }
      }

      result.size shouldBe 1000
      monitoring.inflight() shouldBe 0
      monitoring.queueSize() shouldBe 0

      forAll(result) { data =>
        inside(data) { case None => }
      }
    }
  }
}
