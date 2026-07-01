package com.hiya.alternator.syntax

final class FieldSelector[V] {
  def apply[F](selector: V => F): ConditionExpression.Path[V, F] =
    macro com.hiya.alternator.generic.util.FieldSelectorMacros.field[V, F]
}
