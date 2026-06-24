package com.hiya.alternator

import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.RootDynamoFormat
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
}
