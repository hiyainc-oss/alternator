package com.hiya.alternator.syntax

import com.hiya.alternator.schema.{AttributeValue, ScalarDynamoFormat}

sealed trait ConditionExpression[+T]

object ConditionExpression {

  final class ConditionExpressionBoolExt(val expr: ConditionExpression[Boolean]) extends AnyVal {

    def &&(rhs: ConditionExpression[Boolean]): ConditionExpression[Boolean] =
      ConditionExpression.BinOp("AND", expr, rhs)

    def ||(rhs: ConditionExpression[Boolean]): ConditionExpression[Boolean] =
      ConditionExpression.BinOp("OR", expr, rhs)

    def unary_! : ConditionExpression[Boolean] =
      ConditionExpression.FunCall("NOT", List(expr))
  }

  sealed trait Path[T] extends ConditionExpression[T] {

    def get[U](index: Long): Path[U] = ArrayIndex[U](this, index)
    def get[U](fieldName: String): Path[U] = MapIndex[U](this, fieldName)

    def exists: FunCall[Boolean] = FunCall("attribute_exists", List(this))
    def notExists: FunCall[Boolean] = FunCall("attribute_not_exists", List(this))

    def ===[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, T]): BinOp[Boolean] = BinOp("=", this, ev.expr(rhs))
    def =!=[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, T]): BinOp[Boolean] = BinOp("<>", this, ev.expr(rhs))
    def <[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, T]): BinOp[Boolean] = BinOp("<", this, ev.expr(rhs))
    def >[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, T]): BinOp[Boolean] = BinOp(">", this, ev.expr(rhs))
    def <=[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, T]): BinOp[Boolean] = BinOp("<=", this, ev.expr(rhs))
    def >=[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, T]): BinOp[Boolean] = BinOp(">=", this, ev.expr(rhs))
  }

  trait ExprLike[-From, +To] {
    def expr(from: From): ConditionExpression[To]
  }

  object ExprLike {
    implicit def conditionExpressionIsExprLike[T]: ExprLike[ConditionExpression[T], T] =
      (from: ConditionExpression[T]) => from

    implicit def scalarDynamoFormatIsExprLike[T: ScalarDynamoFormat]: ExprLike[T, T] =
      (from: T) => Literal(from)
  }

  private[alternator] final case class Attr[T](
    name: String
  ) extends Path[T]

  private[alternator] final case class ArrayIndex[T](
    base: Path[_],
    index: Long
  ) extends Path[T]

  private[alternator] final case class MapIndex[T](
    base: Path[_],
    fieldName: String
  ) extends Path[T]

  private[alternator] final case class Literal[T](
    value: T
  )(implicit format: ScalarDynamoFormat[T])
    extends ConditionExpression[T] {
    def write[AV: AttributeValue]: AV = format.write[AV](value)
  }

  private[alternator] object Literal {
    def apply[T: ScalarDynamoFormat](t: T): Literal[T] = new Literal(t)
  }

  private[alternator] final case class FunCall[T](
    name: String,
    args: List[ConditionExpression[_]]
  ) extends ConditionExpression[T]

  private[alternator] final case class BinOp[T](
    op: String,
    lhs: ConditionExpression[_],
    rhs: ConditionExpression[_]
  ) extends ConditionExpression[T]

}
