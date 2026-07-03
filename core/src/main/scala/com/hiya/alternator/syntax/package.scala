package com.hiya.alternator

import cats.{MonadThrow, Traverse}
import com.hiya.alternator.schema.{DynamoFormat, ScalarDynamoFormat, StringLikeDynamoFormat}
import com.hiya.alternator.syntax.ConditionExpression.Path

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

  /** `SET #p = :v`. Bound on the full `DynamoFormat[T]`, not `ScalarDynamoFormat[T]`, since `SET` accepts any attribute
    * value including nested maps.
    */
  def set[V, T: DynamoFormat](path: Path[V, T], value: T): UpdateExpression[V] =
    UpdateExpression(sets = List(UpdateExpression.Assign(path, value)), Nil, Nil, Nil)

  /** `SET #p = list_append(#p, :v)`. */
  def append[V, T: DynamoFormat](path: Path[V, List[T]], values: List[T]): UpdateExpression[V] =
    UpdateExpression(sets = List(UpdateExpression.ListAppend(path, values, prepend = false)), Nil, Nil, Nil)

  /** `SET #p = list_append(:v, #p)`. */
  def prepend[V, T: DynamoFormat](path: Path[V, List[T]], values: List[T]): UpdateExpression[V] =
    UpdateExpression(sets = List(UpdateExpression.ListAppend(path, values, prepend = true)), Nil, Nil, Nil)

  /** `REMOVE #p`. */
  def remove[V](path: Path[V, _]): UpdateExpression[V] =
    UpdateExpression(Nil, removes = List(path), Nil, Nil)

  /** `ADD #p :v` (numeric increment). Native `ADD` is numeric-only, hence `ScalarDynamoFormat` rather than the full
    * `DynamoFormat`; the `Numeric` evidence isn't otherwise needed (DynamoDB does the arithmetic server-side) but is
    * required as a call-site constraint restricting this builder to genuinely numeric `T` — referencing it below keeps
    * `-Wunused`/`-Werror` builds from treating it as dead.
    */
  def increment[V, T](path: Path[V, T], delta: T)(implicit
    N: Numeric[T],
    F: ScalarDynamoFormat[T]
  ): UpdateExpression[V] = {
    val _ = N
    UpdateExpression(Nil, Nil, adds = List(UpdateExpression.Increment(path, delta)), Nil)
  }

  /** `ADD #p :v` (set union). DynamoDB's native Set types (`SS`/`NS`/`BS`) are genuinely scalar-only. */
  def addToSet[V, T: ScalarDynamoFormat](path: Path[V, Set[T]], values: Set[T]): UpdateExpression[V] =
    UpdateExpression(Nil, Nil, adds = List(UpdateExpression.AddToSet(path, values)), Nil)

  /** `DELETE #p :v` (set difference). */
  def removeFromSet[V, T: ScalarDynamoFormat](path: Path[V, Set[T]], values: Set[T]): UpdateExpression[V] =
    UpdateExpression(Nil, Nil, Nil, deletes = List(UpdateExpression.DeleteAction(path, values)))

}
