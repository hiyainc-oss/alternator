package com.example

import com.hiya.alternator.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Lives outside `com.hiya.alternator` on purpose: macro expansions are checked for access at the call site, so
  * anything the macro emits must be public API.
  */
class FieldMacroOutsidePackageSpec extends AnyFunSpec with Matchers {
  case class Inner(b: Int)
  case class Row(a: Inner, c: String)

  describe("field outside com.hiya.alternator") {
    it("should resolve a top-level field") {
      (field[Row](_.c) === "x") shouldBe (attr[String]("c").narrow[Row] === "x")
    }

    it("should resolve a nested field") {
      (field[Row](_.a.b) === 1) shouldBe (attr[Inner]("a").narrow[Row].get[Int]("b") === 1)
    }

    it("should be usable in an update expression") {
      set(field[Row](_.c), "y") shouldBe set(attr[String]("c").narrow[Row], "y")
    }
  }
}
