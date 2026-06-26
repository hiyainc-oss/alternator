package com.hiya.alternator

import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{AttributeValue, DynamoAttributeError, DynamoFormat, RootDynamoFormat}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ProductFormatGoldenSpec extends AnyFunSpec with Matchers {

  case class Simple(id: String, n: Int)
  object Simple {
    implicit val fmt: RootDynamoFormat[Simple] = semiauto.derive
  }

  case class Nested(key: String, inner: Simple)
  object Nested {
    implicit val fmt: RootDynamoFormat[Nested] = semiauto.derive
  }

  case class WithOpt(x: String, y: Option[String])
  object WithOpt {
    implicit val fmt: RootDynamoFormat[WithOpt] = semiauto.derive
  }

  case class WithList(xs: List[Int])
  object WithList {
    implicit val fmt: RootDynamoFormat[WithList] = semiauto.derive
  }

  case class WithMap(m: Map[String, Int])
  object WithMap {
    implicit val fmt: RootDynamoFormat[WithMap] = semiauto.derive
  }

  // Custom scalar: enum stored as a number rather than a string.
  sealed trait Severity
  case object Info extends Severity
  case object Warn extends Severity
  case object Error extends Severity

  object Severity {
    implicit val fmt: DynamoFormat[Severity] = new DynamoFormat[Severity] {
      override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[Severity] =
        AV.numeric(av) match {
          case Some("0") => Right(Info)
          case Some("1") => Right(Warn)
          case Some("2") => Right(Error)
          case _ => Left(DynamoAttributeError.TypeError(av, "Severity"))
        }
      override def write[AV](value: Severity)(implicit AV: AttributeValue[AV]): AV =
        AV.createNumeric(value match {
          case Info  => "0"
          case Warn  => "1"
          case Error => "2"
        })
      override def isEmpty(value: Severity): Boolean = false
    }
  }

  case class LogEntry(msg: String, severity: Severity)
  object LogEntry {
    implicit val fmt: RootDynamoFormat[LogEntry] = semiauto.derive
  }

  /** Build a java.util.Map[String, TestAV] from pairs — explicit type prevents Scala from inferring a concrete subtype
    * as the value type.
    */
  private def jmap(pairs: (String, TestAV)*): java.util.Map[String, TestAV] =
    Map[String, TestAV](pairs: _*).asJava

  describe("product wire format") {

    it("simple case class writes correct fields") {
      Simple.fmt.writeFields[TestAV](Simple("abc", 42)) shouldBe
        jmap("id" -> TAVString("abc"), "n" -> TAVNumber("42"))
    }

    it("simple case class reads back correctly") {
      Simple.fmt.readFields[TestAV](jmap("id" -> TAVString("abc"), "n" -> TAVNumber("42"))) shouldBe
        Right(Simple("abc", 42))
    }

    it("nested case class writes correct fields") {
      Nested.fmt.writeFields[TestAV](Nested("x", Simple("abc", 42))) shouldBe
        jmap(
          "key" -> TAVString("x"),
          "inner" -> TAVMap(Map("id" -> TAVString("abc"), "n" -> TAVNumber("42")))
        )
    }

    it("nested case class reads back correctly") {
      Nested.fmt.readFields[TestAV](
        jmap(
          "key" -> TAVString("x"),
          "inner" -> TAVMap(Map("id" -> TAVString("abc"), "n" -> TAVNumber("42")))
        )
      ) shouldBe Right(Nested("x", Simple("abc", 42)))
    }

    it("Option present — field written") {
      WithOpt.fmt.writeFields[TestAV](WithOpt("a", Some("b"))) shouldBe
        jmap("x" -> TAVString("a"), "y" -> TAVString("b"))
    }

    it("Option absent — field omitted") {
      WithOpt.fmt.writeFields[TestAV](WithOpt("a", None)) shouldBe
        jmap("x" -> TAVString("a"))
    }

    it("Option absent field reads as None") {
      WithOpt.fmt.readFields[TestAV](jmap("x" -> TAVString("a"))) shouldBe
        Right(WithOpt("a", None))
    }

    it("List writes as L") {
      WithList.fmt.writeFields[TestAV](WithList(List(1, 2, 3))) shouldBe
        jmap("xs" -> TAVList(List(TAVNumber("1"), TAVNumber("2"), TAVNumber("3"))))
    }

    it("List reads back correctly") {
      WithList.fmt.readFields[TestAV](
        jmap("xs" -> TAVList(List(TAVNumber("1"), TAVNumber("2"), TAVNumber("3"))))
      ) shouldBe Right(WithList(List(1, 2, 3)))
    }

    it("Map[String,T] writes as M") {
      WithMap.fmt.writeFields[TestAV](WithMap(Map("a" -> 1, "b" -> 2))) shouldBe
        jmap("m" -> TAVMap(Map("a" -> TAVNumber("1"), "b" -> TAVNumber("2"))))
    }

    it("Map[String,T] reads back correctly") {
      WithMap.fmt.readFields[TestAV](
        jmap("m" -> TAVMap(Map("a" -> TAVNumber("1"), "b" -> TAVNumber("2"))))
      ) shouldBe Right(WithMap(Map("a" -> 1, "b" -> 2)))
    }
  }

  describe("custom scalar format in derived product") {

    it("enum field uses the custom number representation on write") {
      LogEntry.fmt.writeFields[TestAV](LogEntry("started", Info)) shouldBe
        jmap("msg" -> TAVString("started"), "severity" -> TAVNumber("0"))
      LogEntry.fmt.writeFields[TestAV](LogEntry("degraded", Warn)) shouldBe
        jmap("msg" -> TAVString("degraded"), "severity" -> TAVNumber("1"))
      LogEntry.fmt.writeFields[TestAV](LogEntry("down", Error)) shouldBe
        jmap("msg" -> TAVString("down"), "severity" -> TAVNumber("2"))
    }

    it("enum field reads back from the number representation") {
      LogEntry.fmt.readFields[TestAV](
        jmap("msg" -> TAVString("started"), "severity" -> TAVNumber("0"))
      ) shouldBe Right(LogEntry("started", Info))
      LogEntry.fmt.readFields[TestAV](
        jmap("msg" -> TAVString("degraded"), "severity" -> TAVNumber("1"))
      ) shouldBe Right(LogEntry("degraded", Warn))
    }

    it("read returns an error for an unrecognised numeric value") {
      // Scala 2 and Scala 3 differ on whether the error is wrapped in FieldFormatError;
      // the important invariant is that an unrecognised value is rejected.
      LogEntry.fmt.readFields[TestAV](
        jmap("msg" -> TAVString("?"), "severity" -> TAVNumber("99"))
      ).isLeft shouldBe true
    }

    it("round-trips through all severity levels") {
      Seq(Info, Warn, Error).foreach { sev =>
        val entry = LogEntry("test", sev)
        LogEntry.fmt.readFields[TestAV](LogEntry.fmt.writeFields[TestAV](entry)) shouldBe Right(entry)
      }
    }
  }
}
