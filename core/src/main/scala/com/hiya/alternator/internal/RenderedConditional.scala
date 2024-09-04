package com.hiya.alternator.internal

import com.hiya.alternator.schema.AttributeValue
import com.hiya.alternator.syntax.ConditionExpression
import com.hiya.alternator.syntax.ConditionExpression._

import java.util
import scala.jdk.CollectionConverters._

private[alternator] trait ConditionalSupport[Builder, AV] {
  def withConditionExpression(builder: Builder, conditionExpression: String): Builder
  def withExpressionAttributeNames(builder: Builder, attributeNames: util.Map[String, String]): Builder
  def withExpressionAttributeValues(builder: Builder, attributeValues: util.Map[String, AV]): Builder
}

private[alternator] object ConditionalSupport {
  def apply[Builder, AV: AttributeValue](builder: Builder, expression: ConditionExpression[Boolean])(implicit
    Builder: ConditionalSupport[Builder, AV]
  ): Builder =
    RenderedConditional.render(expression).apply(builder)
}

private[alternator] final case class RenderedConditional[AV](
  conditionExpression: String,
  attributeNames: Map[String, String],
  attributeValues: Map[String, AV]
) {

  def apply[Builder](builder: Builder)(implicit Builder: ConditionalSupport[Builder, AV]): Builder = {
    val r1 = Builder.withConditionExpression(builder, this.conditionExpression)

    val r2 = if (this.attributeNames.nonEmpty) {
      Builder.withExpressionAttributeNames(r1, this.attributeNames.asJava)
    } else r1

    if (this.attributeValues.nonEmpty) {
      Builder.withExpressionAttributeValues(r2, this.attributeValues.asJava)
    } else r2
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

  private def attributeValuesOf[AV: AttributeValue](expr: ConditionExpression[_]): List[AV] =
    expr match {
      case _: Path[_] => Nil
      case e: Literal[_] => List(e.write[AV])
      case e: FunCall[_] => e.args.flatMap(attributeValuesOf[AV])
      case e: BinOp[_] => attributeValuesOf(e.lhs) ++ attributeValuesOf(e.rhs)
    }

  private[alternator] def render[AV: AttributeValue](expression: ConditionExpression[_]): RenderedConditional[AV] = {
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
