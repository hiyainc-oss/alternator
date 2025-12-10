package com.hiya.alternator

import com.hiya.alternator.internal.{Condition, ConditionParameters}
import com.hiya.alternator.schema.AttributeValue
import com.hiya.alternator.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

abstract class ConditionExpressionTestBase[AV](implicit av: AttributeValue[AV]) extends AnyFunSpec with Matchers {

  final case class RenderedConditional(str: String, params: Map[String, String], values: Map[String, AV])

  def render(expression: ConditionExpression[Boolean]): RenderedConditional = {
    val (params, exp) = Condition.renderCondition(expression).run(ConditionParameters.empty).value
    RenderedConditional(exp, params.names, params.values)
  }

  describe("render") {

    it("should render 'exists'") {
      render(
        attr("Price").exists
      ) shouldBe RenderedConditional(
        "attribute_exists(#a0)",
        Map("#a0" -> "Price"),
        Map.empty
      )
    }

    it("should render 'notExists'") {
      render(
        attr("Price").notExists
      ) shouldBe RenderedConditional(
        "attribute_not_exists(#a0)",
        Map("#a0" -> "Price"),
        Map.empty
      )
    }

    it("should render '='") {
      render(
        attr("Price") === 100
      ) shouldBe RenderedConditional(
        "(#a0) = (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> av.createNumeric("100"))
      )
    }

    it("should render '=' when comparing to another attribute") {
      render(
        attr("Price") === attr("PrevPrice")
      ) shouldBe RenderedConditional(
        "(#a0) = (#a1)",
        Map("#a0" -> "Price", "#a1" -> "PrevPrice"),
        Map.empty
      )
    }

    it("should render '<'") {
      render(
        attr("Price") < 100
      ) shouldBe RenderedConditional(
        "(#a0) < (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> av.createNumeric("100"))
      )
    }

    it("should render '&&'") {
      render(
        attr("Price") < 100 && attr("Category") === "food"
      ) shouldBe RenderedConditional(
        "((#a0) < (:v0)) AND ((#a1) = (:v1))",
        Map("#a0" -> "Price", "#a1" -> "Category"),
        Map(
          ":v0" -> av.createNumeric("100"),
          ":v1" -> av.createString("food")
        )
      )
    }

    it("should render 'not'") {
      render(
        !(attr("Price") < 100 && attr("Category") === "food")
      ) shouldBe RenderedConditional(
        "NOT(((#a0) < (:v0)) AND ((#a1) = (:v1)))",
        Map("#a0" -> "Price", "#a1" -> "Category"),
        Map(
          ":v0" -> av.createNumeric("100"),
          ":v1" -> av.createString("food")
        )
      )
    }

    it("should render multiple occurrences of an attribute name") {
      render(
        attr("Price") >= 50 && attr("Price") <= 100
      ) shouldBe RenderedConditional(
        "((#a0) >= (:v0)) AND ((#a0) <= (:v1))",
        Map("#a0" -> "Price"),
        Map(
          ":v0" -> av.createNumeric("50"),
          ":v1" -> av.createNumeric("100")
        )
      )
    }

    it("should render indexing into an attribute") {
      render(
        attr("Product").get("Pictures").get(0).get("Filename") === "image.png"
      ) shouldBe RenderedConditional(
        "(#a0.#a1[0].#a2) = (:v0)",
        Map("#a0" -> "Product", "#a1" -> "Pictures", "#a2" -> "Filename"),
        Map(":v0" -> av.createString("image.png"))
      )
    }

    it("should render indexing into an attribute name with the same name") {
      render(
        attr("Value").get("Value").exists
      ) shouldBe RenderedConditional(
        "attribute_exists(#a0.#a0)",
        Map("#a0" -> "Value"),
        Map.empty
      )
    }

    it("should render '||' (OR operator)") {
      render(
        attr("Price") < 50 || attr("Category") === "clearance"
      ) shouldBe RenderedConditional(
        "((#a0) < (:v0)) OR ((#a1) = (:v1))",
        Map("#a0" -> "Price", "#a1" -> "Category"),
        Map(
          ":v0" -> av.createNumeric("50"),
          ":v1" -> av.createString("clearance")
        )
      )
    }

    it("should render '=!=' (not equal)") {
      render(
        attr("Status") =!= "deleted"
      ) shouldBe RenderedConditional(
        "(#a0) <> (:v0)",
        Map("#a0" -> "Status"),
        Map(":v0" -> av.createString("deleted"))
      )
    }

    it("should render '>'") {
      render(
        attr("Price") > 100
      ) shouldBe RenderedConditional(
        "(#a0) > (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> av.createNumeric("100"))
      )
    }

    it("should render '>='") {
      render(
        attr("Price") >= 50
      ) shouldBe RenderedConditional(
        "(#a0) >= (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> av.createNumeric("50"))
      )
    }

    it("should render '<='") {
      render(
        attr("Price") <= 100
      ) shouldBe RenderedConditional(
        "(#a0) <= (:v0)",
        Map("#a0" -> "Price"),
        Map(":v0" -> av.createNumeric("100"))
      )
    }

    it("should render complex combination of AND and OR") {
      render(
        (attr("Price") < 50 || attr("Price") > 200) && attr("Category") === "sale"
      ) shouldBe RenderedConditional(
        "(((#a0) < (:v0)) OR ((#a0) > (:v1))) AND ((#a1) = (:v2))",
        Map("#a0" -> "Price", "#a1" -> "Category"),
        Map(
          ":v0" -> av.createNumeric("50"),
          ":v1" -> av.createNumeric("200"),
          ":v2" -> av.createString("sale")
        )
      )
    }

    it("should render attribute comparison with not equal") {
      render(
        attr("CurrentPrice") =!= attr("OriginalPrice")
      ) shouldBe RenderedConditional(
        "(#a0) <> (#a1)",
        Map("#a0" -> "CurrentPrice", "#a1" -> "OriginalPrice"),
        Map.empty
      )
    }
  }
}
