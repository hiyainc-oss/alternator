package com.hiya.alternator

import cats.{MonadThrow, Traverse}
import com.hiya.alternator.schema.{DynamoFormat, ScalarDynamoFormat, StringLikeDynamoFormat}

package object syntax {

  implicit def toTry[T, F[_]: MonadThrow, M[_]: Traverse](
    underlying: F[M[DynamoFormat.Result[T]]]
  ): ThrowErrorsExt[T, F, M] =
    new ThrowErrorsExt[T, F, M](underlying)

  implicit def toTry2[T, F[_]: MonadThrow](
    underlying: F[DynamoFormat.Result[T]]
  ): ThrowErrorsExt2[T, F] =
    new ThrowErrorsExt2[T, F](underlying)

  object rk {
    def <[T: ScalarDynamoFormat](rhs: T): RKCondition.LT[T] = RKCondition.LT[T](rhs)
    def <=[T: ScalarDynamoFormat](rhs: T): RKCondition.LE[T] = RKCondition.LE[T](rhs)
    def >[T: ScalarDynamoFormat](rhs: T): RKCondition.GT[T] = RKCondition.GT[T](rhs)
    def >=[T: ScalarDynamoFormat](rhs: T): RKCondition.GE[T] = RKCondition.GE[T](rhs)
    def ===[T: ScalarDynamoFormat](rhs: T): RKCondition.EQ[T] = RKCondition.EQ[T](rhs)
    def beginsWith[T: StringLikeDynamoFormat](rhs: T): RKCondition.BEGINS_WITH[T] = RKCondition.BEGINS_WITH(rhs)
    def between[T: ScalarDynamoFormat](lower: T, upper: T): RKCondition.BETWEEN[T] = RKCondition.BETWEEN(lower, upper)
  }

  /** Builds an untyped attribute-path condition from a raw field name. Always usable against any table, since the
    * result is not tied to any specific item type — prefer [[field]] where the field is a real member of the item's
    * case class; use `attr` for dynamic field names or attributes not modeled as case class fields.
    */
  def attr[T](name: String): ConditionExpression.Path[Any, T] = ConditionExpression.Attr(name)
  def lit[T: ScalarDynamoFormat](literal: T): ConditionExpression[Any, T] = ConditionExpression.Literal(literal)

  /** Builds a compile-time-checked attribute-path condition from a field-accessor selector, e.g.
    * `field[Person](_.address.city)`. The selector must be a plain accessor chain — arbitrary expressions inside the
    * lambda are a compile error. Unlike [[attr]], the resulting condition can only be used against operations on `V`.
    */
  def field[V]: FieldSelector[V] = new FieldSelector[V]

  implicit def toConditionExpressionBoolExt[V](
    expr: ConditionExpression[V, Boolean]
  ): ConditionExpression.ConditionExpressionBoolExt[V] =
    new ConditionExpression.ConditionExpressionBoolExt[V](expr)

}
