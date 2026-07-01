package com.hiya.alternator.syntax

import cats.kernel.Monoid
import cats.syntax.all._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class UpdateExpressionSpec extends AnyFunSpec with Matchers {
  case class Sample(pk: String, count: Int, tags: Set[String], history: List[String], flag: Boolean)

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
}
