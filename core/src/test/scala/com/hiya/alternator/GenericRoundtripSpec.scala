package com.hiya.alternator

import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.RootDynamoFormat
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** Round-trip tests for generic (type-parameterised) case classes and sealed traits.
  *
  * These tests exercise derivation with concrete type arguments applied at the call site. They run on both Scala 2 (via
  * shapeless LabelledGeneric) and Scala 3 (via Mirror).
  *
  * NOTE: Recursive ADTs (e.g. `case class Tree[A](value: A, children: List[Tree[A]])`) are NOT tested here. Scala 3
  * Mirror derivation does not support recursive types without lazy implicits; this is a known limitation. Scala 2
  * shapeless supports them via `Lazy`, but keeping the test surface consistent avoids cross-version divergence.
  */
class GenericRoundtripSpec extends AnyFunSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  /** Generic product with a single type parameter. */
  case class Box[A](value: A)

  /** Generic product with two type parameters. */
  case class Pair[A, B](first: A, second: B)

  /** Generic sealed trait with two type-parameterised case class variants.
    *
    * We avoid case object variants here to keep the test cross-version compatible: in Scala 2, `semiauto.derive` for a
    * standalone case object requires `ReprDynamoFormat[HNil]` which is not provided. Case objects are covered in
    * SumFormatGoldenSpec where they are used as sum variants (not derived in isolation).
    */
  sealed trait Tagged[A]
  case class WithValue[A](v: A) extends Tagged[A]
  case class WithDefault[A](v: A, label: String) extends Tagged[A]

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def jmap(pairs: (String, TestAV)*): java.util.Map[String, TestAV] =
    Map[String, TestAV](pairs: _*).asJava

  private def roundTrip[A](fmt: RootDynamoFormat[A], value: A): A = {
    val written = fmt.writeFields[TestAV](value)
    fmt.readFields[TestAV](written) match {
      case Right(v) => v
      case Left(err) => fail(s"round-trip read failed: $err")
    }
  }

  // ---------------------------------------------------------------------------
  // Box[A] — generic product
  // ---------------------------------------------------------------------------

  describe("Box[A] — generic product") {

    it("Box[String] writes and reads back (round-trip)") {
      implicit val fmt: RootDynamoFormat[Box[String]] = semiauto.derive
      roundTrip(fmt, Box("hello")) shouldBe Box("hello")
    }

    it("Box[Int] writes and reads back (round-trip)") {
      implicit val fmt: RootDynamoFormat[Box[Int]] = semiauto.derive
      roundTrip(fmt, Box(42)) shouldBe Box(42)
    }

    it("Box[String] writes correct wire format") {
      implicit val fmt: RootDynamoFormat[Box[String]] = semiauto.derive
      fmt.writeFields[TestAV](Box("world")) shouldBe
        jmap("value" -> TAVString("world"))
    }

    it("Box[Option[Int]] round-trip with Some") {
      implicit val fmt: RootDynamoFormat[Box[Option[Int]]] = semiauto.derive
      val value: Box[Option[Int]] = Box(Some(7))
      roundTrip(fmt, value) shouldBe value
    }

    it("Box[Option[Int]] round-trip with None") {
      implicit val fmt: RootDynamoFormat[Box[Option[Int]]] = semiauto.derive
      val value: Box[Option[Int]] = Box(None)
      roundTrip(fmt, value) shouldBe value
    }
  }

  // ---------------------------------------------------------------------------
  // Pair[A, B] — two type parameters
  // ---------------------------------------------------------------------------

  describe("Pair[A, B] — two type parameters") {

    it("Pair[String, Int] round-trip") {
      implicit val fmt: RootDynamoFormat[Pair[String, Int]] = semiauto.derive
      roundTrip(fmt, Pair("key", 99)) shouldBe Pair("key", 99)
    }

    it("Pair[String, Int] writes correct wire format") {
      implicit val fmt: RootDynamoFormat[Pair[String, Int]] = semiauto.derive
      fmt.writeFields[TestAV](Pair("a", 1)) shouldBe
        jmap("first" -> TAVString("a"), "second" -> TAVNumber("1"))
    }
  }

  // ---------------------------------------------------------------------------
  // Tagged[A] — generic sum type
  // ---------------------------------------------------------------------------

  describe("Tagged[A] — generic sealed trait") {

    it("WithValue[String] round-trip") {
      implicit val withValueFmt: RootDynamoFormat[WithValue[String]] = semiauto.derive
      implicit val withDefaultFmt: RootDynamoFormat[WithDefault[String]] = semiauto.derive
      implicit val fmt: RootDynamoFormat[Tagged[String]] = semiauto.derive
      roundTrip(fmt, WithValue("hi")) shouldBe WithValue("hi")
    }

    it("WithDefault[String] round-trip") {
      implicit val withValueFmt: RootDynamoFormat[WithValue[String]] = semiauto.derive
      implicit val withDefaultFmt: RootDynamoFormat[WithDefault[String]] = semiauto.derive
      implicit val fmt: RootDynamoFormat[Tagged[String]] = semiauto.derive
      roundTrip(fmt, WithDefault("hi", "tag")) shouldBe WithDefault("hi", "tag")
    }

    it("WithValue[Int] writes correct wire format") {
      implicit val withValueFmt: RootDynamoFormat[WithValue[Int]] = semiauto.derive
      implicit val withDefaultFmt: RootDynamoFormat[WithDefault[Int]] = semiauto.derive
      implicit val fmt: RootDynamoFormat[Tagged[Int]] = semiauto.derive
      fmt.writeFields[TestAV](WithValue(5)) shouldBe
        jmap("WithValue" -> TAVMap(Map("v" -> TAVNumber("5"))))
    }

    it("user-provided variant format is respected (not re-derived)") {
      // Custom format that writes "v" as uppercase string — verifies that summonVariant
      // picks up the explicit RootDynamoFormat[WithValue[String]] rather than re-deriving.
      // This is the key regression test for Finding #1.
      val customWithValueFmt: RootDynamoFormat[WithValue[String]] =
        new RootDynamoFormat[WithValue[String]] {
          override def writeFields[AV: com.hiya.alternator.schema.AttributeValue](
            value: WithValue[String]
          ): java.util.Map[String, AV] =
            Map(
              "v" -> implicitly[com.hiya.alternator.schema.AttributeValue[AV]].createString(
                value.v.toUpperCase
              )
            ).asJava

          override def readFields[AV: com.hiya.alternator.schema.AttributeValue](
            av: java.util.Map[String, AV]
          ): com.hiya.alternator.schema.DynamoFormat.Result[WithValue[String]] = {
            val AV = implicitly[com.hiya.alternator.schema.AttributeValue[AV]]
            AV.string(av.getOrDefault("v", AV.nullValue))
              .map(s => Right(WithValue(s.toLowerCase)))
              .getOrElse(Left(com.hiya.alternator.schema.DynamoAttributeError.IllegalDiscriminator))
          }
        }

      implicit val withValueFmt: RootDynamoFormat[WithValue[String]] = customWithValueFmt
      implicit val withDefaultFmt: RootDynamoFormat[WithDefault[String]] = semiauto.derive
      implicit val fmt: RootDynamoFormat[Tagged[String]] = semiauto.derive

      // The custom format should uppercase on write
      fmt.writeFields[TestAV](WithValue("hello")) shouldBe
        jmap("WithValue" -> TAVMap(Map("v" -> TAVString("HELLO"))))
    }
  }
}
