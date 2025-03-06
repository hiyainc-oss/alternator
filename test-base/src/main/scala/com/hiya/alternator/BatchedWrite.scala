package com.hiya.alternator

import cats.syntax.all._
import cats.{Id, MonadThrow}
import com.hiya.alternator.util.TableConfig
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should
import org.scalatest.{Inside, Inspectors}

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random

trait BatchedWrite[ClientT <: DynamoDBClient, F[_], S[_]] {
  this: AnyFunSpecLike with should.Matchers with Inspectors with Inside =>

  protected implicit def F: MonadThrow[F]
  protected implicit def writeScheduler: WriteScheduler[F]
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

  trait StreamWritePartiallyApplied[Data, Key] {
    type TTable[C <: DynamoDBClient] = Table[C, Data, Key]
    def apply()(implicit tableConfig: TableConfig[Data, Key, TTable]): Unit
  }

  def streamWrite[Data, Key] = new StreamWritePartiallyApplied[Data, Key] {
    def apply()(implicit tableConfig: TableConfig[Data, Key, TTable]): Unit = {
      def generateData(nums: Int, writes: Int): List[Data] = {
        val state = (0 until nums).map {
          _ -> 0
        }.toArray
        var stateSize = state.length
        val result = List.newBuilder[Data]

        while (stateSize > 0) {
          val i = Random.nextInt(stateSize)
          val (idx, len) = state(i)

          if (len == writes - 1) {
            result += tableConfig.createData(idx)._2
            stateSize -= 1
            state(i) = state(stateSize)
          } else {
            result += tableConfig.createData(idx, Some(idx + Random.nextInt(100)))._2
            state(i) = idx -> (len + 1)
          }
        }

        result.result()
      }

      it("should report if table not exists") {
        val table = tableConfig.table("doesnotexists", stableClient)
        implicit val classTag: ClassTag[ResourceNotFoundException] = resourceNotFoundException

        intercept[ResourceNotFoundException] {
          eval {
            List(1)
              .map(k => tableConfig.createData(k))
              .traverse { case (k, _) => writeScheduler.delete(table.noClient, k) }
          }
        }
      }

      it("should write data") {
        val nums = 100
        val writes = 10

        val (result, data) = eval {
          tableConfig.withTable(stableClient).eval { table =>
            val q = generateData(nums, writes)
            for {
              result <- q.traverse(writeScheduler.put(table.noClient, _))
              data <- (0 until nums)
                .map(k => tableConfig.createData(k))
                .toList
                .traverse { case (key, value) =>
                  DB.get(table, key).map(_ -> value)
                }
            } yield result -> data
          }
        }

        result.size shouldBe nums * writes
        data.size shouldBe nums

        forAll(data) { case (data, pt) =>
          inside(data) { case Some(Right(p)) =>
            p shouldBe pt
          }
        }
      }
    }
  }
}
