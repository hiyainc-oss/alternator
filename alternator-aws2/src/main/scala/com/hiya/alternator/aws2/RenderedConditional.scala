package com.hiya.alternator.aws2

import com.hiya.alternator.syntax.ConditionExpression
import com.hiya.alternator.syntax.ConditionExpression._
import software.amazon.awssdk.services.dynamodb.model
import software.amazon.awssdk.services.dynamodb.model.{DeleteItemRequest, PutItemRequest}

import scala.jdk.CollectionConverters._

private[alternator] final case class RenderedConditional(
  conditionExpression: String,
  attributeNames: Map[String, String],
  attributeValues: Map[String, model.AttributeValue]
) {

  def apply(builder: PutItemRequest.Builder): PutItemRequest.Builder = {
    var result = builder.conditionExpression(this.conditionExpression)
    if (this.attributeNames.nonEmpty) {
      result = builder.expressionAttributeNames(this.attributeNames.asJava)
    }
    if (this.attributeValues.nonEmpty) {
      result = builder.expressionAttributeValues(this.attributeValues.asJava)
    }
    result
  }

  def apply(builder: DeleteItemRequest.Builder): DeleteItemRequest.Builder = {
    var result = builder.conditionExpression(this.conditionExpression)
    if (this.attributeNames.nonEmpty) {
      result = builder.expressionAttributeNames(this.attributeNames.asJava)
    }
    if (this.attributeValues.nonEmpty) {
      result = builder.expressionAttributeValues(this.attributeValues.asJava)
    }
    result
  }
}

object RenderedConditional {

  private def attributeNamesOf(expr: ConditionExpression[_]): Set[String] =
    expr match {
      case e: Attr[_] => Set(e.name)
      case e: ArrayIndex[_] => attributeNamesOf(e.base)
      case e: MapIndex[_] => attributeNamesOf(e.base) + e.fieldName
      case _: Literal[_] => Set.empty
      case e: FunCall[_] => e.args.flatMap(attributeNamesOf).toSet
      case e: BinOp[_] => attributeNamesOf(e.lhs) ++ attributeNamesOf(e.rhs)
    }

  private def attributeValuesOf(expr: ConditionExpression[_]): List[model.AttributeValue] =
    expr match {
      case _: Path[_] => Nil
      case e: Literal[_] => List(e.write[model.AttributeValue])
      case e: FunCall[_] => e.args.flatMap(attributeValuesOf)
      case e: BinOp[_] => attributeValuesOf(e.lhs) ++ attributeValuesOf(e.rhs)
    }

  private[alternator] def render(expression: ConditionExpression[_]): RenderedConditional = {
    val attributeNames = attributeNamesOf(expression).zipWithIndex.map { case (attributeName, i) =>
      (attributeName, "#a" + i.toString)
    }.toMap

    val attributeValues = attributeValuesOf(expression).zipWithIndex.map { case (attributeValue, i) =>
      (attributeValue, ":v" + i.toString)
    }.toMap

    def renderExpr(expression: ConditionExpression[_]): String =
      expression match {
        case Attr(name) => attributeNames(name)
        case ArrayIndex(base, index) => s"${renderExpr(base)}[$index]"
        case MapIndex(base, fieldName) => s"${renderExpr(base)}.${attributeNames(fieldName)}"
        case l @ Literal(_) => attributeValues(l.write)
        case FunCall(name, args) => s"$name(${args.map(renderExpr).mkString(",")})"
        case BinOp(op, lhs, rhs) => s"(${renderExpr(lhs)}) $op (${renderExpr(rhs)})"
      }

    RenderedConditional(
      conditionExpression = renderExpr(expression),
      attributeNames = attributeNames.map { case (k, v) => (v, k) },
      attributeValues = attributeValues.map { case (k, v) => (v, k) }
    )
  }

}
