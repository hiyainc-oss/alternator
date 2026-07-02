package com.hiya.alternator.internal

import cats.data.State
import com.hiya.alternator.ReturnValue
import com.hiya.alternator.schema.AttributeValue
import com.hiya.alternator.syntax.{ConditionExpression, UpdateExpression}

import java.util
import scala.jdk.CollectionConverters._

private[alternator] trait UpdateSupport[Builder, AV] {
  def withUpdateExpression(builder: Builder, updateExpression: String): Builder
  def withExpressionAttributeNames(builder: Builder, attributeNames: util.Map[String, String]): Builder
  def withExpressionAttributeValues(builder: Builder, attributeValues: util.Map[String, AV]): Builder
  def withReturnValues(builder: Builder, returnValue: ReturnValue): Builder
}

private[alternator] object UpdateSupport {
  private def execute[AV, Builder](builder: Builder)(implicit
    Builder: UpdateSupport[Builder, AV]
  ): Condition[AV, Builder] =
    State { params =>
      val withNames =
        if (params.attributeNames.nonEmpty) Builder.withExpressionAttributeNames(builder, params.names.asJava)
        else builder
      val withValues =
        if (params.attributeValues.nonEmpty) Builder.withExpressionAttributeValues(withNames, params.values.asJava)
        else withNames
      (params, withValues)
    }

  private def apply[V, Builder, AV: AttributeValue](builder: Builder, expression: UpdateExpression[V])(implicit
    Builder: UpdateSupport[Builder, AV]
  ): Condition[AV, Builder] =
    for {
      exp <- Condition.renderUpdate(expression)
      ret <- execute(Builder.withUpdateExpression(builder, exp))
    } yield ret

  /** Applies the rendered update expression, an optional condition and an optional `ReturnValues` to `builder` in a
    * single pass. The update and condition expressions '''must''' be rendered within the same `Condition` (`State`)
    * computation and evaluated with a single `Condition.eval` — rendering them independently would call
    * `withExpressionAttributeNames`/`withExpressionAttributeValues` twice, and since those calls replace the whole map
    * on the underlying SDK builder rather than merging into it, the second call would silently discard the first's
    * names/values instead of the two sharing one map as intended.
    */
  def eval[V, Builder, AV: AttributeValue](
    builder: Builder,
    expression: UpdateExpression[V],
    condition: Option[ConditionExpression[V, Boolean]],
    returnValue: Option[ReturnValue]
  )(implicit U: UpdateSupport[Builder, AV], C: ConditionalSupport[Builder, AV]): Builder = {
    val withReturnValue = returnValue.fold(builder)(rv => U.withReturnValues(builder, rv))
    Condition.eval(
      for {
        withUpdate <- apply(withReturnValue, expression)
        result <- condition.fold(Condition.pure[AV, Builder](withUpdate))(cond =>
          ConditionalSupport.apply(withUpdate, cond)
        )
      } yield result
    )
  }
}
