package com.hiya.alternator.internal

import cats.Eval
import cats.data.IndexedStateT
import com.hiya.alternator.schema.AttributeValue
import com.hiya.alternator.syntax.ConditionExpression

import java.util

private[alternator] trait ConditionalSupport[Builder, AV] {
  def withConditionExpression(builder: Builder, conditionExpression: String): Builder
  def withExpressionAttributeNames(builder: Builder, attributeNames: util.Map[String, String]): Builder
  def withExpressionAttributeValues(builder: Builder, attributeValues: util.Map[String, AV]): Builder
}

private[alternator] object ConditionalSupport {
  def apply[Builder, AV: AttributeValue](builder: Builder, expression: ConditionExpression[Boolean])(implicit
    Builder: ConditionalSupport[Builder, AV]
  ): IndexedStateT[Eval, ConditionParameters[AV], ConditionParameters[AV], Builder] =
    for {
      exp <- Condition.renderCondition(expression)
      ret <- Condition.execute(Builder.withConditionExpression(builder, exp))
    } yield ret

  def eval[Builder, AV: AttributeValue](builder: Builder, expression: ConditionExpression[Boolean])(implicit
    Builder: ConditionalSupport[Builder, AV]
  ): Builder =
    Condition.eval(apply(builder, expression))
}
