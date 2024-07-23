package com.hiya.alternator.syntax

import com.hiya.alternator.syntax.ConditionExpression.Rendered
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class ConditionExpressionTest extends AnyFunSpec with Matchers {
  import com.hiya.alternator.syntax.ConditionExpression.render

  describe("render") {

    it("should render 'exists'") {
      render(
        attr("Price").exists
      ) shouldBe Rendered(
        "attribute_exists(#a0)",
        Map("#a0" -> "Price"),
        Map.empty
      )
    }

    it("should render 'notExists'") {
      render(
        attr("Price").notExists
      ) shouldBe Rendered(
        "attribute_not_exists(#a0)",
        Map("#a0" -> "Price"),
        Map.empty
      )
    }

    it("should render '='") {
      render(
        attr("Price") === 100
      ) shouldBe Rendered(
        "(#a0) = (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> AttributeValue.builder().n("100").build())
      )
    }

    it("should render '=' when comparing to another attribute") {
      render(
        attr("Price") === attr("PrevPrice")
      ) shouldBe Rendered(
        "(#a0) = (#a1)",
        Map("#a0" -> "Price", "#a1" -> "PrevPrice"),
        Map.empty
      )
    }

    it("should render '<'") {
      render(
        attr("Price") < 100
      ) shouldBe Rendered(
        "(#a0) < (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> AttributeValue.builder().n("100").build())
      )
    }

    it("should render '&&'") {
      render(
        attr("Price") < 100 && attr("Category") === "food"
      ) shouldBe Rendered(
        "((#a0) < (:v0)) AND ((#a1) = (:v1))",
        Map("#a0" -> "Price", "#a1" -> "Category"),
        Map(
          ":v0" -> AttributeValue.builder().n("100").build(),
          ":v1" -> AttributeValue.builder().s("food").build()
        )
      )
    }

    it("should render 'not'") {
      render(
        !(attr("Price") < 100 && attr("Category") === "food")
      ) shouldBe Rendered(
        "NOT(((#a0) < (:v0)) AND ((#a1) = (:v1)))",
        Map("#a0" -> "Price", "#a1" -> "Category"),
        Map(
          ":v0" -> AttributeValue.builder().n("100").build(),
          ":v1" -> AttributeValue.builder().s("food").build()
        )
      )
    }

    it("should render multiple occurrences of an attribute name") {
      render(
        attr("Price") >= 50 && attr("Price") <= 100
      ) shouldBe Rendered(
        "((#a0) >= (:v0)) AND ((#a0) <= (:v1))",
        Map("#a0" -> "Price"),
        Map(
          ":v0" -> AttributeValue.builder().n("50").build(),
          ":v1" -> AttributeValue.builder().n("100").build()
        )
      )
    }

    it("should render indexing into an attribute") {
      render(
        attr("Product").get("Pictures").get(0).get("Filename") === "image.png"
      ) shouldBe Rendered(
        "(#a0.#a1[0].#a2) = (:v0)",
        Map("#a0" -> "Product", "#a1" -> "Pictures", "#a2" -> "Filename"),
        Map(":v0" -> AttributeValue.builder().s(("image.png")).build())
      )
    }

    it("should render indexing into an attribute name with the same name") {
      render(
        attr("Value").get("Value").exists
      ) shouldBe Rendered(
        "attribute_exists(#a0.#a0)",
        Map("#a0" -> "Value"),
        Map.empty
      )
    }
  }
}
