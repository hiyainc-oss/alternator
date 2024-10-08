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

  def attr[T](name: String): ConditionExpression.Path[T] = ConditionExpression.Attr(name)
  def lit[T: ScalarDynamoFormat](literal: T): ConditionExpression[T] = ConditionExpression.Literal(literal)

  implicit def toConditionExpressionBoolExt(
    expr: ConditionExpression[Boolean]
  ): ConditionExpression.ConditionExpressionBoolExt =
    new ConditionExpression.ConditionExpressionBoolExt(expr)

}
