package com.hiya.alternator.syntax

import com.hiya.alternator.schema.{AttributeValue, ScalarDynamoFormat}

sealed trait ConditionExpression[-V, +T]

object ConditionExpression {

  final class ConditionExpressionBoolExt[V](val expr: ConditionExpression[V, Boolean]) extends AnyVal {

    def &&(rhs: ConditionExpression[V, Boolean]): ConditionExpression[V, Boolean] =
      ConditionExpression.BinOp("AND", expr, rhs)

    def ||(rhs: ConditionExpression[V, Boolean]): ConditionExpression[V, Boolean] =
      ConditionExpression.BinOp("OR", expr, rhs)

    def unary_! : ConditionExpression[V, Boolean] =
      ConditionExpression.FunCall("NOT", List(expr))
  }

  sealed trait Path[V, T] extends ConditionExpression[V, T] {

    def get[U](index: Long): Path[V, U] = ArrayIndex[V, U](this, index)
    def get[U](fieldName: String): Path[V, U] = MapIndex[V, U](this, fieldName)

    def exists: FunCall[V, Boolean] = FunCall("attribute_exists", List(this))
    def notExists: FunCall[V, Boolean] = FunCall("attribute_not_exists", List(this))

    def ===[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, V, T]): BinOp[V, Boolean] = BinOp("=", this, ev.expr(rhs))
    def =!=[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, V, T]): BinOp[V, Boolean] = BinOp("<>", this, ev.expr(rhs))
    def <[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, V, T]): BinOp[V, Boolean] = BinOp("<", this, ev.expr(rhs))
    def >[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, V, T]): BinOp[V, Boolean] = BinOp(">", this, ev.expr(rhs))
    def <=[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, V, T]): BinOp[V, Boolean] = BinOp("<=", this, ev.expr(rhs))
    def >=[Rhs](rhs: Rhs)(implicit ev: ExprLike[Rhs, V, T]): BinOp[V, Boolean] = BinOp(">=", this, ev.expr(rhs))
  }

  trait ExprLike[-From, V, +To] {
    def expr(from: From): ConditionExpression[V, To]
  }

  object ExprLike {
    implicit def conditionExpressionIsExprLike[V, T]: ExprLike[ConditionExpression[V, T], V, T] =
      (from: ConditionExpression[V, T]) => from

    implicit def scalarDynamoFormatIsExprLike[V, T: ScalarDynamoFormat]: ExprLike[T, V, T] =
      (from: T) => Literal(from)
  }

  private[alternator] final case class Attr[V, T](
    name: String
  ) extends Path[V, T]

  private[alternator] final case class ArrayIndex[V, T](
    base: Path[V, _],
    index: Long
  ) extends Path[V, T]

  private[alternator] final case class MapIndex[V, T](
    base: Path[V, _],
    fieldName: String
  ) extends Path[V, T]

  private[alternator] final case class Literal[T](
    value: T
  )(implicit format: ScalarDynamoFormat[T])
    extends ConditionExpression[Any, T] {
    def write[AV: AttributeValue]: AV = format.write[AV](value)
  }

  private[alternator] object Literal {
    def apply[T: ScalarDynamoFormat](t: T): Literal[T] = new Literal(t)
  }

  private[alternator] final case class FunCall[V, T](
    name: String,
    args: List[ConditionExpression[V, _]]
  ) extends ConditionExpression[V, T]

  private[alternator] final case class BinOp[V, T](
    op: String,
    lhs: ConditionExpression[V, _],
    rhs: ConditionExpression[V, _]
  ) extends ConditionExpression[V, T]

}
