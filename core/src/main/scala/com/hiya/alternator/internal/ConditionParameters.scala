package com.hiya.alternator.internal

import cats.data.State
import cats.syntax.all._
import com.hiya.alternator.schema.AttributeValue
import com.hiya.alternator.syntax.ConditionExpression
import com.hiya.alternator.syntax.ConditionExpression._

import scala.jdk.CollectionConverters._

private[alternator] final case class ConditionParameters[AV](
  attributeNames: Map[String, String],
  attributeValues: Map[AV, String]
) {
  def apply[Builder](builder: Builder)(implicit Builder: ConditionalSupport[Builder, AV]): Builder = {
    val r2 = if (this.attributeNames.nonEmpty) {
      Builder.withExpressionAttributeNames(builder, names.asJava)
    } else builder

    if (this.attributeValues.nonEmpty) {
      Builder.withExpressionAttributeValues(r2, values.asJava)
    } else r2
  }

  def names: Map[String, String] = this.attributeNames.map(_.swap)
  def values: Map[String, AV] = this.attributeValues.map(_.swap)
}

private[alternator] object ConditionParameters {
  def empty[AV]: ConditionParameters[AV] = ConditionParameters(Map.empty, Map.empty)

  def execute[AV, Builder](builder: Builder)(implicit
    Builder: ConditionalSupport[Builder, AV]
  ): Condition[AV, Builder] =
    State { params => (params, params(builder)) }

  def getParam[AV](name: String): Condition[AV, String] =
    State { params =>
      val idx = params.attributeNames.getOrElse(name, s"#a${params.attributeNames.size}")
      (params.copy(attributeNames = params.attributeNames.updated(name, idx)), idx)
    }

  def getValue[AV](value: AV): Condition[AV, String] =
    State { params =>
      val idx = params.attributeValues.getOrElse(value, s":v${params.attributeValues.size}")
      (params.copy(attributeValues = params.attributeValues.updated(value, idx)), idx)
    }

  def renderCondition[AV: AttributeValue](expression: ConditionExpression[_]): Condition[AV, String] =
    expression match {
      case Attr(name) =>
        getParam(name)

      case ArrayIndex(base, index) =>
        for {
          expr <- renderCondition[AV](base)
        } yield s"$expr[$index]"

      case MapIndex(base, fieldName) =>
        for {
          expr <- renderCondition[AV](base)
          fieldName <- getParam[AV](fieldName)
        } yield s"$expr.$fieldName"

      case l @ Literal(_) =>
        getValue(l.write[AV])

      case FunCall(name, args) =>
        for {
          args <- args.traverse(renderCondition[AV])
        } yield s"$name(${args.mkString(",")})"

      case BinOp(op, lhs, rhs) =>
        for {
          lhs <- renderCondition[AV](lhs)
          rhs <- renderCondition[AV](rhs)
        } yield s"($lhs) $op ($rhs)"
    }

}
