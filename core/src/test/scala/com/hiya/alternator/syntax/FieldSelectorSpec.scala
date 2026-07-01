package com.hiya.alternator.syntax

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class FieldSelectorSpec extends AnyFunSpec with Matchers {
  case class Sample(key: String, value: Int)

  describe("field") {
    it("should build the same Attr as attr() for a top-level field") {
      val viaField = field[Sample](_.value)
      viaField shouldBe ConditionExpression.Attr[Sample, Int]("value")
    }

    it("should produce a working condition via ===") {
      val cond = field[Sample](_.value) === 1000
      cond shouldBe ConditionExpression.BinOp[Sample, Boolean](
        "=",
        ConditionExpression.Attr[Sample, Int]("value"),
        ConditionExpression.Literal(1000)
      )
    }

    it("should fail to compile for a nonexistent field") {
      assertDoesNotCompile("""field[Sample](_.nonexistent)""")
    }
  }
}
