package com.hiya.alternator.syntax

import com.hiya.alternator.DynamoFormat
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

sealed trait ConditionExpression[+T]

object ConditionExpression {

  final implicit class ConditionExpressionBoolExt(expr: ConditionExpression[Boolean]) {

    def &&(rhs: ConditionExpression[Boolean]): ConditionExpression[Boolean] =
      ConditionExpression.BinOp("AND", expr, rhs)

    def ||(rhs: ConditionExpression[Boolean]): ConditionExpression[Boolean] =
      ConditionExpression.BinOp("OR", expr, rhs)

    def not: ConditionExpression[Boolean] =
      ConditionExpression.FunCall("NOT", List(expr))
  }

  sealed trait Path[T] extends ConditionExpression[T] {

    def get[U](index: Long): Path[U]       = ArrayIndex[U](this, index)
    def get[U](fieldName: String): Path[U] = MapIndex[U](this, fieldName)

    def exists: FunCall[Boolean]    = FunCall("attribute_exists", List(this))
    def notExists: FunCall[Boolean] = FunCall("attribute_not_exists", List(this))

    def ===(rhs: ConditionExpression[T]): BinOp[Boolean] = BinOp("=", this, rhs)
    def =!=(rhs: ConditionExpression[T]): BinOp[Boolean] = BinOp("<>", this, rhs)
    def <(rhs: ConditionExpression[T]): BinOp[Boolean]   = BinOp("<", this, rhs)
    def >(rhs: ConditionExpression[T]): BinOp[Boolean]   = BinOp(">", this, rhs)
    def <=(rhs: ConditionExpression[T]): BinOp[Boolean]  = BinOp("<=", this, rhs)
    def >=(rhs: ConditionExpression[T]): BinOp[Boolean]  = BinOp(">=", this, rhs)
  }

  private[syntax] final case class Attr[T](
    name: String
  ) extends Path[T]

  private[syntax] final case class ArrayIndex[T](
    base: Path[_],
    index: Long
  ) extends Path[T]

  private[syntax] final case class MapIndex[T](
    base: Path[_],
    fieldName: String
  ) extends Path[T]

  private[syntax] final case class Literal[T](
    attributeValue: AttributeValue
  ) extends ConditionExpression[T]

  private[syntax] object Literal {
    def apply[T: DynamoFormat](t: T): Literal[T] = Literal(implicitly[DynamoFormat[T]].write(t))
  }

  private[syntax] final case class FunCall[T](
    name: String,
    args: List[ConditionExpression[_]]
  ) extends ConditionExpression[T]

  private[syntax] final case class BinOp[T](
    op: String,
    lhs: ConditionExpression[_],
    rhs: ConditionExpression[_]
  ) extends ConditionExpression[T]

  private[alternator] final case class Rendered(
    conditionExpression: String,
    attributeNames: Map[String, String],
    attributeValues: Map[String, AttributeValue]
  ) {

    def apply(builder: PutItemRequest.Builder): PutItemRequest.Builder = {
      import com.hiya.alternator.CollectionConvertersCompat.mapAsJava

      var result = builder.conditionExpression(this.conditionExpression)
      if (this.attributeNames.nonEmpty) {
        result = builder.expressionAttributeNames(mapAsJava(this.attributeNames))
      }
      if (this.attributeValues.nonEmpty) {
        result = builder.expressionAttributeValues(mapAsJava(this.attributeValues))
      }
      result
    }
  }

  private def attributeNamesOf(expr: ConditionExpression[_]): Set[String] =
    expr match {
      case e: Attr[_]       => Set(e.name)
      case e: ArrayIndex[_] => attributeNamesOf(e.base)
      case e: MapIndex[_]   => attributeNamesOf(e.base) + e.fieldName
      case _: Literal[_]    => Set.empty
      case e: FunCall[_]    => e.args.flatMap(attributeNamesOf).toSet
      case e: BinOp[_]      => attributeNamesOf(e.lhs) ++ attributeNamesOf(e.rhs)
    }

  private def attributeValuesOf(expr: ConditionExpression[_]): List[AttributeValue] =
    expr match {
      case _: Path[_]    => Nil
      case e: Literal[_] => List(e.attributeValue)
      case e: FunCall[_] => e.args.flatMap(attributeValuesOf)
      case e: BinOp[_]   => attributeValuesOf(e.lhs) ++ attributeValuesOf(e.rhs)
    }

  private[alternator] def render(expression: ConditionExpression[_]): Rendered = {
    val attributeNames = attributeNamesOf(expression).zipWithIndex.map {
      case (attributeName, i) => (attributeName, "#a" + i.toString)
    }.toMap

    val attributeValues = attributeValuesOf(expression).zipWithIndex.map {
      case (attributeValue, i) => (attributeValue, ":v" + i.toString)
    }.toMap

    def renderExpr(expression: ConditionExpression[_]): String =
      expression match {
        case Attr(name)                => attributeNames(name)
        case ArrayIndex(base, index)   => s"${renderExpr(base)}[$index]"
        case MapIndex(base, fieldName) => s"${renderExpr(base)}.${attributeNames(fieldName)}"
        case Literal(value)            => attributeValues(value)
        case FunCall(name, args)       => s"$name(${args.map(renderExpr).mkString(",")})"
        case BinOp(op, lhs, rhs)       => s"(${renderExpr(lhs)}) $op (${renderExpr(rhs)})"
      }

    Rendered(
      conditionExpression = renderExpr(expression),
      attributeNames = attributeNames.map { case (k, v) => (v, k) },
      attributeValues = attributeValues.map { case (k, v) => (v, k) }
    )
  }
}
