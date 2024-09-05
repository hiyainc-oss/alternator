package com.hiya.alternator

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{RootDynamoFormat, TableSchema}
import com.hiya.alternator.syntax._
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.util.{DataPK, DataRK}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should

import java.util.UUID

object DynamoDBTestBase {
  case class ExampleData(pk: String, intValue: Int, stringValue: String)
  object ExampleData {
    implicit val format: RootDynamoFormat[ExampleData] = semiauto.derive
    implicit val schema: TableSchema.Aux[ExampleData, String] =
      TableSchema.schemaWithPK[ExampleData, String]("pk", _.pk)
  }
}

abstract class DynamoDBTestBase[F[_], S[_], C] extends AnyFunSpecLike with should.Matchers {

  import DynamoDBTestBase._

  protected def client: C
  protected implicit def dbr: DynamoDB[F, S, C]
  protected implicit def monadF: MonadThrow[F]
  protected implicit def monadS: MonadThrow[S]

  protected def eval[T](body: F[T]): T
  protected def list[T](body: S[T]): F[List[T]]

  describe("DynamoDB") {
    it("can read/write items in custom test table") {
      val tableName = s"test-table-${UUID.randomUUID()}"
      val key = "primaryKey"
      val data = ExampleData(key, 12, "string value")

      val exampleDBInstance = Table.tableWithPK[ExampleData](tableName).withClient(client)
      val exampleDBInstance2 = Table.tableWithPK[ExampleData](tableName).withClient(client)

      eval {
        LocalDynamoDB.withTable(client, tableName, LocalDynamoDB.schema[ExampleData]).eval { _ =>
          for {
            _ <- toTry(exampleDBInstance.get[F](key)).raiseError.map(_ shouldBe None)
            _ <- exampleDBInstance.put[F](data).map(_ shouldBe (()))

            _ <- exampleDBInstance.get[F](key).raiseError.map(_ shouldBe Some(data))
            _ <- exampleDBInstance2.get[F](key).raiseError.map(_ shouldBe Some(data))

            _ <- exampleDBInstance.delete[F](key).map(_ shouldBe (()))
            _ <- exampleDBInstance.get[F](key).map(_ shouldBe None)
            _ <- exampleDBInstance2.get[F](key).map(_ shouldBe None)
          } yield ()
        }
      }
    }
  }

  describe("query") {
    def withRangeData[T](
      num: Int,
      payload: Option[String] = None
    )(
      f: TableWithRangeKeyLike[C, DataRK, String, String] => S[T]
    ): List[T] =
      eval {
        list {
          DataRK.config.withTable(client).source { table =>
            DynamoDB[F, S, C]
              .eval {
                List(num, 11).traverse { i =>
                  (0 until num).toList.traverse { j =>
                    table.put[F](DataRK(i.toString, j.toString, payload.getOrElse(s"$i/$j")))
                  }
                }
              }
              .flatMap(_ => f(table))
          }
        }
      }

    it("should compile =") {
      val result = withRangeData(5) { table =>
        table.query(pk = "5", rk === "3").raiseError
      }

      result shouldBe List(DataRK("5", "3", "5/3"))
    }

    it("should compile <") {
      val result = withRangeData(5) { table =>
        table.query(pk = "5", rk < "3").raiseError
      }

      result shouldBe (0 until 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile <=") {
      val result = withRangeData(5) { table =>
        table.query(pk = "5", rk <= "3").raiseError
      }

      result shouldBe (0 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >") {
      val result = withRangeData(5) { table =>
        table.query(pk = "5", rk > "3").raiseError
      }

      result shouldBe (4 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >=") {
      val result = withRangeData(5) { table =>
        table.query(pk = "5", rk >= "3").raiseError
      }

      result shouldBe (3 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile between") {
      val result = withRangeData(5) { table =>
        table.query(pk = "5", rk.between("2", "3")).raiseError
      }

      result shouldBe (2 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile startswith") {
      val result = withRangeData(13) { table =>
        table.query(pk = "13", rk.beginsWith("1")).raiseError
      }

      result shouldBe (1 :: (10 until 13).toList).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work without rk condition") {
      val result = withRangeData(13) { table =>
        table.query(pk = "13").raiseError
      }

      result should contain theSameElementsAs (0 until 13).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work with a lots of data") {
      val payload = "0123456789abcdefghijklmnopqrstuvwxyz" * 1000
      val result = withRangeData(1000, payload = Some(payload)) { table =>
        table.query(pk = "1000").raiseError
      }

      result should have size 1000
    }

    it("should work with limit") {
      val result = withRangeData(1000) { table =>
        table.query(pk = "1000", limit = 500.some).raiseError
      }

      result should have size 500
    }

    it("should filter with non-key and range condition") {
      val result = withRangeData(13) { table =>
        table.query(pk = "13", rk < "5", condition = Some(attr("value") === "13/1")).raiseError
      }

      result shouldBe List(DataRK("13", "1", "13/1"))
    }

    it("should filter with non-key condition") {
      val result = withRangeData(13) { table =>
        table.query(pk = "13", condition = Some(attr("value") === "13/1")).raiseError
      }

      result shouldBe List(DataRK("13", "1", "13/1"))
    }
  }

  describe("put") {
    it("should work with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table.putAndReturn[F](DataPK("new", 1000)).map(_ shouldBe None) >>
            table.putAndReturn[F](DataPK("new", 1001)).map(_ shouldBe Some(Right(DataPK("new", 1000))))
        }
      }
    }
  }

  describe("put with condition") {
    it("should work for insert-if-not-exists") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table.put(DataPK("new", 1000), attr("key").notExists).map(_ shouldBe true) >>
            table.put(DataPK("new", 1000), attr("key").notExists).map(_ shouldBe false)
        }
      }
    }

    it("should work for optimistic locking") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table.put(DataPK("new", 1000), attr("key").notExists).map(_ shouldBe true) >>
            table.put(DataPK("new", 1001), attr("value") === 1000).map(_ shouldBe true) >>
            table.put(DataPK("new", 1001), attr("value") === 1000).map(_ shouldBe false)
        }
      }
    }

    it("should work for optimistic locking with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table
            .putAndReturn(DataPK("new", 1000), attr("key").notExists)
            .map(_ shouldBe ConditionResult.Success(None)) >>
            table
              .putAndReturn(DataPK("new", 1001), attr("value") === 1000)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("new", 1000))))) >>
            table.putAndReturn(DataPK("new", 1001), attr("value") === 1000).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }
  }

  describe("delete") {
    it("should work with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table.deleteAndReturn[F]("new").map(_ shouldBe None) >>
            table.put[F](DataPK("new", 1)) >>
            table.deleteAndReturn[F]("new").map(_ shouldBe Some(Right(DataPK("new", 1))))
        }
      }
    }
  }

  describe("delete with condition") {
    it("should work with checked delete") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table.put[F](DataPK("new", 1)) >>
            table.delete("new", attr("value") === 2).map(_ shouldBe false) >>
            table.delete("new", attr("value") === 1).map(_ shouldBe true) >>
            table.delete("new", attr("value") === 1).map(_ shouldBe false)
        }
      }
    }

    it("should work with checked delete with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          table.put[F](DataPK("new", 1)) >>
            table.deleteAndReturn("new", attr("value") === 2).map(_ shouldBe ConditionResult.Failed) >>
            table
              .deleteAndReturn("new", attr("value") === 1)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("new", 1))))) >>
            table.deleteAndReturn("new", attr("value") === 1).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }
  }

  describe("scan") {
    def withData[T](f: TableLike[C, DataPK, String] => F[T]): T = eval {
      DataPK.config.withTable(client).eval { table =>
        (1 to 1000).toList
          .map(i => DataPK(i.toString, i))
          .traverse { data =>
            table.put[F](data)
          } >> f(table)
      }
    }

    it("should scan table twice") {
      withData { table =>
        list(table.scan()).map(_.size shouldBe 1000) >>
          list(table.scan()).map(_.size shouldBe 1000)
      }
    }

    it("should filter with condition") {
      val result = withData { table =>
        list(table.scan(condition = Some(attr[Int]("value") === 330)))
      }

      result shouldBe List(Right(DataPK("330", 330)))
    }

    it("should work with limit") {
      val result = withData { table =>
        list(table.scan(limit = 500.some)).raiseError
      }

      result should have size 500
    }

  }
}
