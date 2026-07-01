package com.hiya.alternator.syntax

import com.hiya.alternator.generic.util.FieldSelectorMacros

final class FieldSelector[V] {
  inline def apply[F](inline selector: V => F): ConditionExpression.Path[V, F] =
    ${ FieldSelectorMacros.field[V, F]('selector) }
}
