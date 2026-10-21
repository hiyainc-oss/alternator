package com.hiya.alternator.syntax

import cats.kernel.Monoid
import cats.syntax.all._
import com.hiya.alternator.TestAV
import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.internal.{Condition, ConditionParameters}
import com.hiya.alternator.schema.{RootDynamoFormat, TableSchema}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class UpdateExpressionSpec extends AnyFunSpec with Matchers {
  case class Sample(pk: String, count: Int, tags: Set[String], history: List[String], flag: Boolean)

  case class Row(pk: String, count: Int, note: Option[String], tags: Set[String], history: List[String])
  object Row {
    implicit val format: RootDynamoFormat[Row] = semiauto.derive
    implicit val schema: TableSchema[Row] = TableSchema.schemaWithPK[Row, String]("pk", _.pk)
  }

  case class RangeRow(pk: String, rk: String, value: String)
  object RangeRow {
    implicit val format: RootDynamoFormat[RangeRow] = semiauto.derive
    implicit val schema: TableSchema[RangeRow] =
      TableSchema.schemaWithRK[RangeRow, String, String]("pk", "rk", x => x.pk -> x.rk)
  }

  case class OptSet(pk: String, labels: Option[Set[String]])
  object OptSet {
    implicit val format: RootDynamoFormat[OptSet] = semiauto.derive
    implicit val schema: TableSchema[OptSet] = TableSchema.schemaWithPK[OptSet, String]("pk", _.pk)
  }

  private def render[V](update: UpdateExpression[V]): (String, Map[String, String], Map[String, TestAV]) = {
    val (params, exp) = Condition.renderUpdate[TestAV, V](update).run(ConditionParameters.empty[TestAV]).value
    (exp, params.names, params.values)
  }

  private val setCount: UpdateExpression[Sample] = set(field[Sample](_.count), 1)
  private val incrCount: UpdateExpression[Sample] = increment(field[Sample](_.count), 1)
  private val removeFlag: UpdateExpression[Sample] = remove(field[Sample](_.flag))

  describe("Monoid[UpdateExpression[V]]") {
    it("has an empty that is a no-op on the left and right") {
      (Monoid[UpdateExpression[Sample]].empty |+| setCount) shouldBe setCount
      (setCount |+| Monoid[UpdateExpression[Sample]].empty) shouldBe setCount
    }

    it("combines associatively") {
      ((setCount |+| incrCount) |+| removeFlag) shouldBe (setCount |+| (incrCount |+| removeFlag))
    }

    it("concatenates each clause bucket independently") {
      val combined = setCount |+| incrCount |+| removeFlag
      combined.sets shouldBe setCount.sets
      combined.adds shouldBe incrCount.adds
      combined.removes shouldBe removeFlag.removes
      combined.deletes shouldBe empty
    }
  }

  describe("setAll") {
    it("should SET every non-key attribute in name order") {
      render(setAll(Row("a", 1, Some("n"), Set("x"), Nil))) shouldBe (
        (
          "SET #a0 = :v0, #a1 = :v1, #a2 = :v2, #a3 = :v3",
          Map("#a0" -> "count", "#a1" -> "history", "#a2" -> "note", "#a3" -> "tags"),
          Map(
            ":v0" -> TAVNumber("1"),
            ":v1" -> TAVList(Nil),
            ":v2" -> TAVString("n"),
            ":v3" -> TAVStringSet(Set("x"))
          )
        )
      )
    }

    it("should skip None and empty sets so they are left untouched") {
      val (exp, names, values) = render(setAll(Row("a", 1, None, Set.empty, List("h"))))
      exp shouldBe "SET #a0 = :v0, #a1 = :v1"
      names shouldBe Map("#a0" -> "count", "#a1" -> "history")
      values shouldBe Map(":v0" -> TAVNumber("1"), ":v1" -> TAVList(List(TAVString("h"))))
    }

    it("should skip attributes that serialize to NULL") {
      render(setAll(OptSet("a", Some(Set.empty)))) shouldBe (("", Map.empty, Map.empty))
    }

    it("should exclude both partition and range key") {
      render(setAll(RangeRow("a", "b", "v"))) shouldBe (
        ("SET #a0 = :v0", Map("#a0" -> "value"), Map(":v0" -> TAVString("v")))
      )
    }

    it("should render an empty update when nothing is left to write") {
      render(setAll(OptSet("a", None))) shouldBe (("", Map.empty, Map.empty))
    }

    it("should share one names/values map with other actions") {
      val (exp, names, values) =
        render(
          setAll(Row("a", 1, None, Set.empty, Nil)) |+|
            remove(attr[String]("legacy").narrow[Row]) |+|
            increment(attr[Int]("hits").narrow[Row], 2)
        )
      exp shouldBe "SET #a0 = :v0, #a1 = :v1 REMOVE #a2 ADD #a3 :v2"
      names shouldBe Map("#a0" -> "count", "#a1" -> "history", "#a2" -> "legacy", "#a3" -> "hits")
      values shouldBe Map(":v0" -> TAVNumber("1"), ":v1" -> TAVList(Nil), ":v2" -> TAVNumber("2"))
    }

    it("should not be structurally empty even if it renders to nothing") {
      setAll(OptSet("a", None)).isEmpty shouldBe false
    }
  }
}
