package com.hiya.alternator.syntax

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class NestedFieldSelectorSpec extends AnyFunSpec with Matchers {
  case class Zip(code: String)
  case class Address(city: String, zip: Zip)
  case class Person(name: String, address: Address)

  describe("field with a nested selector") {
    it("should build a MapIndex wrapping an Attr") {
      val path = field[Person](_.address.city)
      path shouldBe ConditionExpression.MapIndex[Person, String](
        ConditionExpression.Attr[Person, Address]("address"),
        "city"
      )
    }

    it("should build a MapIndex chain for a three-segment selector") {
      val path = field[Person](_.address.zip.code)
      path shouldBe ConditionExpression.MapIndex[Person, String](
        ConditionExpression.MapIndex[Person, Zip](
          ConditionExpression.Attr[Person, Address]("address"),
          "zip"
        ),
        "code"
      )
    }
  }
}
