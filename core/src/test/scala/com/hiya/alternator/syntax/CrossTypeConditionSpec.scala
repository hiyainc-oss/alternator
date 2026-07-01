package com.hiya.alternator.syntax

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.unused

class CrossTypeConditionSpec extends AnyFunSpec with Matchers {
  case class Foo(value: Int)
  case class Bar(value: Int)

  // Mirrors the shape of DynamoDB.scala's `condition: ConditionExpression[V, Boolean]`
  // parameter without depending on any concrete DynamoDB client/effect module.
  // The parameter is intentionally unused: this method exists only to be
  // typechecked at each call site below, never actually invoked at runtime.
  def useCondition[V](@unused condition: ConditionExpression[V, Boolean]): Boolean = true

  describe("field[V] conditions") {
    it("should not compile when used where a different V is expected") {
      assertDoesNotCompile("""useCondition[Bar](field[Foo](_.value) === 1)""")
    }

    it("should compile when used against the matching V") {
      assertCompiles("""useCondition[Foo](field[Foo](_.value) === 1)""")
    }

    it("should fail to compile on a type-mismatched comparison") {
      assertDoesNotCompile("""field[Foo](_.value) === "not an int"""")
    }

    it("should still accept attr(...) as an untyped escape hatch for any V") {
      assertCompiles("""useCondition[Foo](attr("value") === 1)""")
      assertCompiles("""useCondition[Bar](attr("value") === 1)""")
    }
  }
}
