package com.hiya.alternator

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{IndexSchema, IndexSchemaWithRange, RootDynamoFormat, TableSchema}
import com.hiya.alternator.syntax._
import com.hiya.alternator.testkit.{LocalDynamoDB, TestContainerInitializer}
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

abstract class DynamoDBTestBase[F[_], S[_], C <: DynamoDBClient]
  extends AnyFunSpecLike
  with should.Matchers
  with TestContainerInitializer {

  import DynamoDBTestBase._

  protected def client: C
  protected implicit def DB: DynamoDB.Aux[F, S, C]
  protected implicit def monadF: MonadThrow[F]
  protected implicit def monadS: MonadThrow[S]

  protected def eval[T](body: F[T]): T
  protected def list[T](body: S[T]): F[List[T]]

  describe("DynamoDB") {
    it("can read/write items in custom test table") {
      val tableName = s"test-table-${UUID.randomUUID()}"
      val key = "primaryKey"
      val data = ExampleData(key, 12, "string value")

      val exampleDBInstance = Table.tableWithPK[ExampleData](tableName).withClient[C](client)
      val exampleDBInstance2 = Table.tableWithPK[ExampleData](tableName).withClient[C](client)

      eval {
        LocalDynamoDB.withTable(client, tableName, LocalDynamoDB.schema[ExampleData]).eval { _ =>
          for {
            _ <- toTry(DB.get(exampleDBInstance, key)).raiseError.map(_ shouldBe None)
            _ <- DB.put(exampleDBInstance, data).map(_ shouldBe (()))

            _ <- DB.get(exampleDBInstance, key).raiseError.map(_ shouldBe Some(data))
            _ <- DB.get(exampleDBInstance2, key).raiseError.map(_ shouldBe Some(data))

            _ <- DB.delete(exampleDBInstance, key).map(_ shouldBe (()))
            _ <- DB.get(exampleDBInstance, key).map(_ shouldBe None)
            _ <- DB.get(exampleDBInstance2, key).map(_ shouldBe None)
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
      f: TableWithRange[C, DataRK, String, String] => S[T]
    ): List[T] =
      eval {
        list {
          DataRK.config.withTable(client).source { table =>
            DB.eval {
              List(num, 11).traverse { i =>
                (0 until num).toList.traverse { j =>
                  DB.put(table, DataRK(i.toString, j.toString, payload.getOrElse(s"$i/$j")))
                }
              }
            }.flatMap(_ => f(table))
          }
        }
      }

    it("should compile =") {
      val result = withRangeData(5) { table =>
        DB.query(table, pk = "5", rk === "3").raiseError
      }

      result shouldBe List(DataRK("5", "3", "5/3"))
    }

    it("should compile <") {
      val result = withRangeData(5) { table =>
        DB.query(table, pk = "5", rk < "3").raiseError
      }

      result shouldBe (0 until 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile <=") {
      val result = withRangeData(5) { table =>
        DB.query(table, pk = "5", rk <= "3").raiseError
      }

      result shouldBe (0 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >") {
      val result = withRangeData(5) { table =>
        DB.query(table, pk = "5", rk > "3").raiseError
      }

      result shouldBe (4 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile >=") {
      val result = withRangeData(5) { table =>
        DB.query(table, pk = "5", rk >= "3").raiseError
      }

      result shouldBe (3 until 5).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile between") {
      val result = withRangeData(5) { table =>
        DB.query(table, pk = "5", rk.between("2", "3")).raiseError
      }

      result shouldBe (2 to 3).map { j => DataRK("5", s"$j", s"5/$j") }
    }

    it("should compile startswith") {
      val result = withRangeData(13) { table =>
        DB.query(table, pk = "13", rk.beginsWith("1")).raiseError
      }

      result shouldBe (1 :: (10 until 13).toList).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work without rk condition") {
      val result = withRangeData(13) { table =>
        DB.query(table, pk = "13").raiseError
      }

      result should contain theSameElementsAs (0 until 13).map { j => DataRK("13", s"$j", s"13/$j") }
    }

    it("should work with a lots of data") {
      val payload = "0123456789abcdefghijklmnopqrstuvwxyz" * 1000
      val result = withRangeData(1000, payload = Some(payload)) { table =>
        DB.query(table, pk = "1000").raiseError
      }

      result should have size 1000
    }

    it("should work with limit") {
      val result = withRangeData(1000) { table =>
        DB.query(table, pk = "1000", limit = 500.some).raiseError
      }

      result should have size 500
    }

    it("should filter with non-key and range condition") {
      val result = withRangeData(13) { table =>
        DB.query(table, pk = "13", rk < "5", condition = Some(attr("value") === "13/1")).raiseError
      }

      result shouldBe List(DataRK("13", "1", "13/1"))
    }

    it("should filter with non-key condition") {
      val result = withRangeData(13) { table =>
        DB.query(table, pk = "13", condition = Some(attr("value") === "13/1")).raiseError
      }

      result shouldBe List(DataRK("13", "1", "13/1"))
    }
  }

  describe("put") {
    it("should work with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.putAndReturn(table, DataPK("new", 1000)).map(_ shouldBe None) >>
            DB.putAndReturn(table, DataPK("new", 1001)).map(_ shouldBe Some(Right(DataPK("new", 1000))))
        }
      }
    }
  }

  describe("put with condition") {
    it("should work for insert-if-not-exists") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("new", 1000), attr("key").notExists).map(_ shouldBe true) >>
            DB.put(table, DataPK("new", 1000), attr("key").notExists).map(_ shouldBe false)
        }
      }
    }

    it("should work for optimistic locking") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("new", 1000), attr("key").notExists).map(_ shouldBe true) >>
            DB.put(table, DataPK("new", 1001), attr("value") === 1000).map(_ shouldBe true) >>
            DB.put(table, DataPK("new", 1001), attr("value") === 1000).map(_ shouldBe false)
        }
      }
    }

    it("should work for optimistic locking with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.putAndReturn(table, DataPK("new", 1000), attr("key").notExists)
            .map(_ shouldBe ConditionResult.Success(None)) >>
            DB.putAndReturn(table, DataPK("new", 1001), attr("value") === 1000)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("new", 1000))))) >>
            DB.putAndReturn(table, DataPK("new", 1001), attr("value") === 1000).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }
  }

  describe("delete") {
    it("should work with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.deleteAndReturn(table, "new").map(_ shouldBe None) >>
            DB.put(table, DataPK("new", 1)) >>
            DB.deleteAndReturn(table, "new").map(_ shouldBe Some(Right(DataPK("new", 1))))
        }
      }
    }
  }

  describe("delete with condition") {
    it("should work with checked delete") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("new", 1)) >>
            DB.delete(table, "new", condition = attr("value") === 2).map(_ shouldBe false) >>
            DB.delete(table, "new", condition = attr("value") === 1).map(_ shouldBe true) >>
            DB.delete(table, "new", condition = attr("value") === 1).map(_ shouldBe false)
        }
      }
    }

    it("should work with checked delete with return") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("new", 1)) >>
            DB.deleteAndReturn(table, "new", condition = attr("value") === 2).map(_ shouldBe ConditionResult.Failed) >>
            DB.deleteAndReturn(table, "new", condition = attr("value") === 1)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("new", 1))))) >>
            DB.deleteAndReturn(table, "new", condition = attr("value") === 1).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }
  }

  describe("conditional operations with diverse expressions") {
    it("should handle complex AND conditions that match") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("complex", 100)) >>
            DB.putAndReturn(
              table,
              DataPK("complex", 200),
              (attr("value") === 100) && (attr("key") === "complex")
            ).map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("complex", 100)))))
        }
      }
    }

    it("should handle complex AND conditions that fail") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("complex", 100)) >>
            DB.putAndReturn(
              table,
              DataPK("complex", 200),
              (attr("value") === 100) && (attr("key") === "wrong")
            ).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should handle OR conditions that match on first clause") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("or-test", 50)) >>
            DB.putAndReturn(
              table,
              DataPK("or-test", 75),
              (attr("value") === 50) || (attr("value") === 999)
            ).map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("or-test", 50)))))
        }
      }
    }

    it("should handle OR conditions that match on second clause") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("or-test", 50)) >>
            DB.putAndReturn(
              table,
              DataPK("or-test", 75),
              (attr("value") === 999) || (attr("value") === 50)
            ).map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("or-test", 50)))))
        }
      }
    }

    it("should handle OR conditions that fail on all clauses") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("or-test", 50)) >>
            DB.putAndReturn(
              table,
              DataPK("or-test", 75),
              (attr("value") === 999) || (attr("value") === 888)
            ).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should handle comparison operators with success") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("compare", 50)) >>
            DB.deleteAndReturn(table, "compare", condition = attr("value") > 25)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("compare", 50)))))
        }
      }
    }

    it("should handle comparison operators with failure") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("compare", 50)) >>
            DB.deleteAndReturn(table, "compare", condition = attr("value") > 100)
              .map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should handle not-equal operator with success") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("neq", 50)) >>
            DB.deleteAndReturn(table, "neq", condition = attr("value") =!= 999)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("neq", 50)))))
        }
      }
    }

    it("should handle not-equal operator with failure") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("neq", 50)) >>
            DB.deleteAndReturn(table, "neq", condition = attr("value") =!= 50)
              .map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should handle attribute_exists with success") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("exists", 1)) >>
            DB.putAndReturn(table, DataPK("exists", 2), attr("value").exists)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("exists", 1)))))
        }
      }
    }

    it("should handle attribute_exists with failure") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("exists", 1)) >>
            DB.putAndReturn(table, DataPK("exists", 2), attr("nonexistent").exists)
              .map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should handle attribute_not_exists with success") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("notexists", 1)) >>
            DB.putAndReturn(table, DataPK("notexists", 2), attr("nonexistent").notExists)
              .map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("notexists", 1)))))
        }
      }
    }

    it("should handle attribute_not_exists with failure") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("notexists", 1)) >>
            DB.putAndReturn(table, DataPK("notexists", 2), attr("value").notExists)
              .map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should handle complex AND+OR combinations that match") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("combo", 100)) >>
            DB.deleteAndReturn(
              table,
              "combo",
              condition = ((attr("value") > 50) && (attr("key") === "combo")) || (attr("value") === 999)
            ).map(_ shouldBe ConditionResult.Success(Some(Right(DataPK("combo", 100)))))
        }
      }
    }

    it("should handle complex AND+OR combinations that fail") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          DB.put(table, DataPK("combo", 100)) >>
            DB.deleteAndReturn(
              table,
              "combo",
              condition = ((attr("value") > 50) && (attr("key") === "wrong")) || (attr("value") === 999)
            ).map(_ shouldBe ConditionResult.Failed)
        }
      }
    }

    it("should distinguish between boolean and ConditionResult return types") {
      eval {
        DataPK.config.withTable(client).eval { table =>
          for {
            _ <- DB.put(table, DataPK("ret-type", 10))
            // Boolean return: condition fails -> false
            boolResult <- DB.put(table, DataPK("ret-type", 20), attr("value") === 999)
            _ = boolResult shouldBe false
            // ConditionResult return: condition fails -> ConditionResult.Failed
            condResult <- DB.putAndReturn(table, DataPK("ret-type", 30), attr("value") === 999)
            _ = condResult shouldBe ConditionResult.Failed
            // Boolean return: condition succeeds -> true
            boolSuccess <- DB.put(table, DataPK("ret-type", 40), attr("value") === 10)
            _ = boolSuccess shouldBe true
            // ConditionResult return: condition succeeds -> ConditionResult.Success with old value
            condSuccess <- DB.putAndReturn(table, DataPK("ret-type", 50), attr("value") === 40)
          } yield condSuccess shouldBe ConditionResult.Success(Some(Right(DataPK("ret-type", 40))))
        }
      }
    }
  }

  describe("scan") {
    def withData[T](f: Table[C, DataPK, String] => F[T]): T = eval {
      DataPK.config.withTable(client).eval { table =>
        (1 to 1000).toList
          .map(i => DataPK(i.toString, i))
          .traverse { data =>
            DB.put(table, data)
          } >> f(table)
      }
    }

    it("should scan table twice") {
      withData { table =>
        list(DB.scan(table)).map(_.size shouldBe 1000) >>
          list(DB.scan(table)).map(_.size shouldBe 1000)
      }
    }

    it("should filter with condition") {
      val result = withData { table =>
        list(DB.scan(table, condition = Some(attr[Int]("value") === 330)))
      }

      result shouldBe List(Right(DataPK("330", 330)))
    }

    it("should work with limit") {
      val result = withData { table =>
        list(DB.scan(table, limit = 500.some)).raiseError
      }

      result should have size 500
    }
  }

  describe("index with partition key") {
    val indexSchema = IndexSchema.onTable(ExampleData.schema)("myindex", "intValue", _.intValue)

    val data = List(
      ExampleData("100", 1, "a"),
      ExampleData("101", 1, "b"),
      ExampleData("200", 2, "c")
    )

    it("can be queried") {
      val tableName = s"test-table-${UUID.randomUUID()}"
      val table = Table.tableWithPK[ExampleData](tableName).withClient[C](client)
      val index = table.index(indexSchema)

      eval {
        LocalDynamoDB
          .withTable(client, tableName, LocalDynamoDB.schema[ExampleData].withIndex(indexSchema))
          .eval { _ =>
            for {
              _ <- data.traverse_(record => DB.put(table, record))

              _ <- list(DB.queryPK(index, 1)).map(
                _ should contain theSameElementsAs data.take(2).map(Right(_))
              )
              _ <- list(DB.queryPK(index, 2)).map(
                _ should contain theSameElementsAs data.drop(2).map(Right(_))
              )
            } yield ()
          }
      }
    }

    it("can be scanned") {
      val tableName = s"test-table-${UUID.randomUUID()}"
      val table = Table.tableWithPK[ExampleData](tableName).withClient[C](client)
      val index = table.index(indexSchema)

      eval {
        LocalDynamoDB
          .withTable(client, tableName, LocalDynamoDB.schema[ExampleData].withIndex(indexSchema))
          .eval { _ =>
            for {
              _ <- data.traverse_(record => DB.put(table, record))
              _ <- list(DB.scan(index)).map(_ should contain theSameElementsAs data.map(Right(_)))
            } yield ()
          }
      }
    }
  }

  describe("index with range key") {
    val indexSchema = IndexSchemaWithRange.onTable(
      ExampleData.schema
    )("myindex", "intValue", "stringValue", d => (d.intValue, d.stringValue))

    val data = List(
      ExampleData("100", 1, "a"),
      ExampleData("101", 1, "a"),
      ExampleData("200", 2, "c")
    )

    it("can be queried") {
      val tableName = s"test-table-${UUID.randomUUID()}"
      val table = Table.tableWithPK[ExampleData](tableName).withClient[C](client)
      val index = table.index(indexSchema)

      eval {
        LocalDynamoDB
          .withTable(client, tableName, LocalDynamoDB.schema[ExampleData].withIndex(indexSchema))
          .eval { _ =>
            for {
              _ <- data.traverse_(row => DB.put(table, row))

              _ <- list(DB.query(index, 1, rk beginsWith "a")).map(
                _ should contain theSameElementsAs data.take(2).map(Right(_))
              )
              _ <- list(DB.query(index, 2, rk beginsWith "c")).map(
                _ should contain theSameElementsAs data.drop(2).map(Right(_))
              )
            } yield ()
          }
      }
    }

    it("can be scanned") {
      val tableName = s"test-table-${UUID.randomUUID()}"
      val table = Table.tableWithPK[ExampleData](tableName).withClient[C](client)
      val index = table.index(indexSchema)

      eval {
        LocalDynamoDB
          .withTable(client, tableName, LocalDynamoDB.schema[ExampleData].withIndex(indexSchema))
          .eval { _ =>
            for {
              _ <- data.traverse_(row => DB.put(table, row))
              _ <- list(DB.scan(index)).map(_ should contain theSameElementsAs data.map(Right(_)))
            } yield ()
          }
      }
    }
  }
}
