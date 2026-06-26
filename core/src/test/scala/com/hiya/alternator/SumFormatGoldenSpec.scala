package com.hiya.alternator

import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.RootDynamoFormat
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class SumFormatGoldenSpec extends AnyFunSpec with Matchers {

  sealed trait Shape
  case class Circle(radius: Double) extends Shape
  case class Rect(w: Double, h: Double) extends Shape
  object Shape {
    implicit val circleFmt: RootDynamoFormat[Circle] = semiauto.derive
    implicit val rectFmt: RootDynamoFormat[Rect] = semiauto.derive
    implicit val fmt: RootDynamoFormat[Shape] = semiauto.derive
  }

  sealed trait Status
  case object Active extends Status
  case object Inactive extends Status
  object Status {
    implicit val fmt: RootDynamoFormat[Status] = semiauto.derive
  }

  sealed trait Event
  case object Ping extends Event
  case class Message(text: String) extends Event
  object Event {
    implicit val msgFmt: RootDynamoFormat[Message] = semiauto.derive
    implicit val fmt: RootDynamoFormat[Event] = semiauto.derive
  }

  /** Build a java.util.Map[String, TestAV] from pairs — explicit type prevents Scala from inferring a concrete subtype
    * as the value type.
    */
  private def jmap(pairs: (String, TestAV)*): java.util.Map[String, TestAV] =
    Map[String, TestAV](pairs: _*).asJava

  describe("sealed trait — case class variants") {

    it("Circle writes with variant name as key, fields as nested map") {
      Shape.fmt.writeFields[TestAV](Circle(1.0)) shouldBe
        jmap("Circle" -> TAVMap(Map("radius" -> TAVNumber("1.0"))))
    }

    it("Circle reads back correctly") {
      Shape.fmt.readFields[TestAV](
        jmap("Circle" -> TAVMap(Map("radius" -> TAVNumber("1.0"))))
      ) shouldBe Right(Circle(1.0))
    }

    it("Rect writes with variant name as key, fields as nested map") {
      Shape.fmt.writeFields[TestAV](Rect(2.0, 3.0)) shouldBe
        jmap("Rect" -> TAVMap(Map("w" -> TAVNumber("2.0"), "h" -> TAVNumber("3.0"))))
    }

    it("Rect reads back correctly") {
      Shape.fmt.readFields[TestAV](
        jmap("Rect" -> TAVMap(Map("w" -> TAVNumber("2.0"), "h" -> TAVNumber("3.0"))))
      ) shouldBe Right(Rect(2.0, 3.0))
    }
  }

  describe("sealed trait — case object variants") {

    it("Active writes as { VariantName -> string(VariantName) }") {
      Status.fmt.writeFields[TestAV](Active) shouldBe
        jmap("Active" -> TAVString("Active"))
    }

    it("Active reads back correctly") {
      Status.fmt.readFields[TestAV](jmap("Active" -> TAVString("Active"))) shouldBe
        Right(Active)
    }

    it("Inactive writes as { VariantName -> string(VariantName) }") {
      Status.fmt.writeFields[TestAV](Inactive) shouldBe
        jmap("Inactive" -> TAVString("Inactive"))
    }

    it("Inactive reads back correctly") {
      Status.fmt.readFields[TestAV](jmap("Inactive" -> TAVString("Inactive"))) shouldBe
        Right(Inactive)
    }
  }

  describe("sealed trait — mixed case class and case object variants") {

    it("Ping (case object) writes as string") {
      Event.fmt.writeFields[TestAV](Ping) shouldBe
        jmap("Ping" -> TAVString("Ping"))
    }

    it("Ping reads back correctly") {
      Event.fmt.readFields[TestAV](jmap("Ping" -> TAVString("Ping"))) shouldBe
        Right(Ping)
    }

    it("Message (case class) writes as nested map") {
      Event.fmt.writeFields[TestAV](Message("hello")) shouldBe
        jmap("Message" -> TAVMap(Map("text" -> TAVString("hello"))))
    }

    it("Message reads back correctly") {
      Event.fmt.readFields[TestAV](
        jmap("Message" -> TAVMap(Map("text" -> TAVString("hello"))))
      ) shouldBe Right(Message("hello"))
    }
  }
}
