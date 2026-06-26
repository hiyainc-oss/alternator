package com.hiya.alternator

import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{AttributeValue, DynamoFormat, RootDynamoFormat}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters._

/** Golden tests for sealed-trait hierarchies with intermediate sealed-trait variants (grouping ADTs).
  *
  * Scala 3 Mirror.SumOf for `Expr` produces `(Arithmetic, Lit)` as the direct subtypes, where `Arithmetic` is itself
  * a sealed trait. Derivation must handle this recursively, mirroring Scala 2 shapeless behaviour where `CConsFormat`
  * summons `Lazy[DynamoFormat[HV]]` for each coproduct element regardless of whether it is a product or sum type.
  *
  * Wire format: each nesting level wraps in an extra map keyed by the variant name.
  *   Add(1, 2) → { "Arithmetic": { "Add": { "a": 1, "b": 2 } } }
  *   Lit(5)    → { "Lit": { "n": 5 } }
  */
class NestedSumFormatSpec extends AnyFunSpec with Matchers {

  // -------------------------------------------------------------------------
  // Two-level ADT with a grouping sealed trait
  // -------------------------------------------------------------------------

  sealed trait Expr
  sealed trait Arithmetic extends Expr
  case class Add(a: Int, b: Int) extends Arithmetic
  case class Mul(a: Int, b: Int) extends Arithmetic
  case class Lit(n: Int) extends Expr

  object Expr {
    implicit val addFmt: RootDynamoFormat[Add] = semiauto.derive
    implicit val mulFmt: RootDynamoFormat[Mul] = semiauto.derive
    implicit val arithmeticFmt: RootDynamoFormat[Arithmetic] = semiauto.derive
    implicit val litFmt: RootDynamoFormat[Lit] = semiauto.derive
    implicit val fmt: RootDynamoFormat[Expr] = semiauto.derive
  }

  import Expr._

  private def jmap(pairs: (String, TestAV)*): java.util.Map[String, TestAV] =
    Map[String, TestAV](pairs: _*).asJava

  // -------------------------------------------------------------------------
  // Arithmetic as a standalone Root format
  // -------------------------------------------------------------------------

  describe("Arithmetic — nested sealed trait derivable as standalone RootDynamoFormat") {

    it("Add writes with variant name as key, fields as nested map") {
      arithmeticFmt.writeFields[TestAV](Add(1, 2)) shouldBe
        jmap("Add" -> TAVMap(Map("a" -> TAVNumber("1"), "b" -> TAVNumber("2"))))
    }

    it("Add reads back correctly from Arithmetic format") {
      arithmeticFmt.readFields[TestAV](
        jmap("Add" -> TAVMap(Map("a" -> TAVNumber("1"), "b" -> TAVNumber("2"))))
      ) shouldBe Right(Add(1, 2))
    }

    it("Mul writes with variant name as key") {
      arithmeticFmt.writeFields[TestAV](Mul(3, 4)) shouldBe
        jmap("Mul" -> TAVMap(Map("a" -> TAVNumber("3"), "b" -> TAVNumber("4"))))
    }
  }

  // -------------------------------------------------------------------------
  // Expr format — Arithmetic variant (nested sum)
  // -------------------------------------------------------------------------

  describe("Expr — Arithmetic variant (nested sealed trait)") {

    it("Add writes with two levels of nesting: Arithmetic > Add > fields") {
      fmt.writeFields[TestAV](Add(1, 2)) shouldBe
        jmap("Arithmetic" -> TAVMap(Map("Add" -> TAVMap(Map("a" -> TAVNumber("1"), "b" -> TAVNumber("2"))))))
    }

    it("Add reads back correctly through Expr format") {
      fmt.readFields[TestAV](
        jmap("Arithmetic" -> TAVMap(Map("Add" -> TAVMap(Map("a" -> TAVNumber("1"), "b" -> TAVNumber("2"))))))
      ) shouldBe Right(Add(1, 2))
    }

    it("Mul writes with two levels of nesting: Arithmetic > Mul > fields") {
      fmt.writeFields[TestAV](Mul(3, 4)) shouldBe
        jmap("Arithmetic" -> TAVMap(Map("Mul" -> TAVMap(Map("a" -> TAVNumber("3"), "b" -> TAVNumber("4"))))))
    }

    it("Mul reads back correctly through Expr format") {
      fmt.readFields[TestAV](
        jmap("Arithmetic" -> TAVMap(Map("Mul" -> TAVMap(Map("a" -> TAVNumber("3"), "b" -> TAVNumber("4"))))))
      ) shouldBe Right(Mul(3, 4))
    }
  }

  // -------------------------------------------------------------------------
  // Expr format — flat Lit variant
  // -------------------------------------------------------------------------

  describe("Expr — Lit variant (direct case class)") {

    it("Lit writes with variant name as key, fields as nested map") {
      fmt.writeFields[TestAV](Lit(5)) shouldBe
        jmap("Lit" -> TAVMap(Map("n" -> TAVNumber("5"))))
    }

    it("Lit reads back correctly through Expr format") {
      fmt.readFields[TestAV](
        jmap("Lit" -> TAVMap(Map("n" -> TAVNumber("5"))))
      ) shouldBe Right(Lit(5))
    }
  }

  // -------------------------------------------------------------------------
  // Round-trips
  // -------------------------------------------------------------------------

  describe("round-trip through Expr format") {

    def roundTrip(value: Expr): Expr = {
      val written = fmt.writeFields[TestAV](value)
      fmt.readFields[TestAV](written) match {
        case Right(v) => v
        case Left(err) => fail(s"round-trip read failed: $err")
      }
    }

    it("Add round-trips") { roundTrip(Add(1, 2)) shouldBe Add(1, 2) }
    it("Mul round-trips") { roundTrip(Mul(3, 4)) shouldBe Mul(3, 4) }
    it("Lit round-trips") { roundTrip(Lit(5)) shouldBe Lit(5) }
  }

  // -------------------------------------------------------------------------
  // User-provided format for nested sum is respected
  // -------------------------------------------------------------------------

  describe("user-provided RootDynamoFormat for nested sum overrides structural derivation") {

    it("custom Arithmetic format is used when deriving Expr") {
      val customArithmetic: RootDynamoFormat[Arithmetic] = new RootDynamoFormat[Arithmetic] {
        override def writeFields[AV: AttributeValue](value: Arithmetic): util.Map[String, AV] =
          Map("_custom" -> summon[AttributeValue[AV]].createString("yes")).asJava
        override def readFields[AV: AttributeValue](av: util.Map[String, AV]): DynamoFormat.Result[Arithmetic] =
          Right(Add(0, 0))
      }

      implicit val overriddenArith: RootDynamoFormat[Arithmetic] = customArithmetic
      implicit val overriddenLit: RootDynamoFormat[Lit] = semiauto.derive
      val exprFmt: RootDynamoFormat[Expr] = semiauto.derive

      exprFmt.writeFields[TestAV](Add(1, 2)) shouldBe
        jmap("Arithmetic" -> TAVMap(Map("_custom" -> TAVString("yes"))))
    }
  }
}
