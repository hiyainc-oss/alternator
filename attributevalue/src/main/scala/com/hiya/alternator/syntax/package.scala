package com.hiya.alternator

import cats.Traverse
import com.hiya.alternator.util.MonadErrorThrowable

package object syntax {

  implicit def toTry[T, F[_] : MonadErrorThrowable, M[_] : Traverse](underlying: F[M[DynamoFormat.Result[T]]]): ThrowErrorsExt[T, F, M] =
    new ThrowErrorsExt[T, F, M](underlying)


  object rk {
    def <[T : ScalarDynamoFormat](rhs: T): RKCondition.LT[T] = RKCondition.LT[T](rhs)
    def <=[T : ScalarDynamoFormat](rhs: T): RKCondition.LE[T] = RKCondition.LE[T](rhs)
    def >[T : ScalarDynamoFormat](rhs: T): RKCondition.GT[T] = RKCondition.GT[T](rhs)
    def >=[T : ScalarDynamoFormat](rhs: T): RKCondition.GE[T] = RKCondition.GE[T](rhs)
    def ==[T : ScalarDynamoFormat](rhs: T): RKCondition.EQ[T] = RKCondition.EQ[T](rhs)
    def beginsWith[T : StringLikeDynamoFormat](rhs: T): RKCondition.BEGINS_WITH[T] = RKCondition.BEGINS_WITH(rhs)
    def between[T : ScalarDynamoFormat](lower: T, upper: T): RKCondition.BETWEEN[T] = RKCondition.BETWEEN(lower, upper)
  }


}
