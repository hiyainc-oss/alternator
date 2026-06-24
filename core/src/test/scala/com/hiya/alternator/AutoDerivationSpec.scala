package com.hiya.alternator

import com.hiya.alternator.TestAV._
import com.hiya.alternator.generic.auto._
import com.hiya.alternator.schema.RootDynamoFormat
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class AutoDerivationSpec extends AnyFunSpec with Matchers {

  case class Item(id: String, value: Int)

  sealed trait Tag
  case object TagA extends Tag
  case class TagB(label: String) extends Tag

  /** Build a java.util.Map[String, TestAV] from pairs — explicit type prevents Scala from inferring a concrete subtype
    * as the value type.
    */
  private def jmap(pairs: (String, TestAV)*): java.util.Map[String, TestAV] =
    Map[String, TestAV](pairs: _*).asJava

  describe("auto derivation") {

    it("derives RootDynamoFormat for a product type via auto import") {
      val fmt = implicitly[RootDynamoFormat[Item]]
      fmt.writeFields[TestAV](Item("x", 1)) shouldBe
        jmap("id" -> TAVString("x"), "value" -> TAVNumber("1"))
    }

    it("derives RootDynamoFormat for a sum type via auto import") {
      val fmt = implicitly[RootDynamoFormat[Tag]]
      fmt.writeFields[TestAV](TagA) shouldBe
        jmap("TagA" -> TAVString("TagA"))
      fmt.writeFields[TestAV](TagB("foo")) shouldBe
        jmap("TagB" -> TAVMap(Map("label" -> TAVString("foo"))))
    }

    it("explicit implicit val takes priority over auto") {
      // Use implicit val (not given) so this test compiles on both Scala 2 and Scala 3
      implicit val explicitFmt: RootDynamoFormat[Item] = new RootDynamoFormat[Item] {
        override def readFields[AV: com.hiya.alternator.schema.AttributeValue](
          av: java.util.Map[String, AV]
        ) =
          Right(Item("explicit", 0))
        override def writeFields[AV: com.hiya.alternator.schema.AttributeValue](value: Item) =
          Map.empty[String, AV].asJava
      }
      implicitly[RootDynamoFormat[Item]] shouldBe explicitFmt
    }
  }
}
