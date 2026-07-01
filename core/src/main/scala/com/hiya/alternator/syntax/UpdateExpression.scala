package com.hiya.alternator.syntax

import cats.kernel.Monoid
import com.hiya.alternator.internal.Condition
import com.hiya.alternator.schema.{AttributeValue, DynamoFormat, ScalarDynamoFormat, ScalarType}

/** Flat data holder for a DynamoDB `UpdateExpression`: one list per clause type (`SET`/`REMOVE`/`ADD`/`DELETE`), since
  * each clause may appear at most once per request, in that fixed order. Compose with cats' `|+|`; it's plain list
  * concatenation (`Monoid`), not boolean logic, and comes with `combineAll`/`empty` for free.
  *
  * '''Known limitation''': the `Monoid` guarantees clause ordering, not path uniqueness — e.g. `set(p, 1) |+|
  * remove(p)` typechecks but DynamoDB rejects the resulting request at call time.
  */
final case class UpdateExpression[V](
  sets: List[UpdateExpression.SetAction[V, _]],
  removes: List[ConditionExpression.Path[V, _]],
  adds: List[UpdateExpression.AddAction[V, _]],
  deletes: List[UpdateExpression.DeleteAction[V, _]]
)

object UpdateExpression {
  implicit def monoid[V]: Monoid[UpdateExpression[V]] = new Monoid[UpdateExpression[V]] {
    def empty: UpdateExpression[V] = UpdateExpression(Nil, Nil, Nil, Nil)
    def combine(x: UpdateExpression[V], y: UpdateExpression[V]): UpdateExpression[V] =
      UpdateExpression(x.sets ++ y.sets, x.removes ++ y.removes, x.adds ++ y.adds, x.deletes ++ y.deletes)
  }

  private[alternator] sealed trait SetAction[V, T] {
    def path: ConditionExpression.Path[V, T]
    def renderValue[AV: AttributeValue](pathExpr: String): Condition[AV, String]
  }

  private[alternator] final case class Assign[V, T](path: ConditionExpression.Path[V, T], value: T)(implicit
    format: DynamoFormat[T]
  ) extends SetAction[V, T] {
    override def renderValue[AV: AttributeValue](pathExpr: String): Condition[AV, String] =
      Condition.getValue(format.write[AV](value))
  }

  private[alternator] final case class ListAppend[V, T](
    path: ConditionExpression.Path[V, List[T]],
    values: List[T],
    prepend: Boolean
  )(implicit format: DynamoFormat[T])
    extends SetAction[V, List[T]] {
    override def renderValue[AV: AttributeValue](pathExpr: String): Condition[AV, String] =
      Condition.getValue(DynamoFormat.listDynamoFormat[T](format).write[AV](values)).map { valueExpr =>
        if (prepend) s"list_append($valueExpr, $pathExpr)" else s"list_append($pathExpr, $valueExpr)"
      }
  }

  private[alternator] sealed trait AddAction[V, T] {
    def path: ConditionExpression.Path[V, T]
    def value[AV: AttributeValue]: AV
  }

  private[alternator] final case class Increment[V, T](path: ConditionExpression.Path[V, T], delta: T)(implicit
    format: ScalarDynamoFormat[T]
  ) extends AddAction[V, T] {
    override def value[AV: AttributeValue]: AV = format.write[AV](delta)
  }

  private[alternator] final case class AddToSet[V, T](path: ConditionExpression.Path[V, Set[T]], values: Set[T])(
    implicit format: ScalarDynamoFormat[T]
  ) extends AddAction[V, Set[T]] {
    override def value[AV: AttributeValue]: AV = ScalarSetFormat.write(values)
  }

  private[alternator] final case class DeleteAction[V, T](path: ConditionExpression.Path[V, Set[T]], values: Set[T])(
    implicit format: ScalarDynamoFormat[T]
  ) {
    def value[AV: AttributeValue]: AV = ScalarSetFormat.write(values)
  }

  /** Builds the native `SS`/`NS`/`BS` attribute value for a `Set[T]` from just `T`'s `ScalarDynamoFormat`, dispatching
    * on `attributeType` — there's no generic `DynamoFormat[Set[T]]` instance covering every `ScalarDynamoFormat[T]`
    * (e.g. `Array[Byte]` only has a `Set[ByteBuffer]` instance), so `ADD`/`DELETE`'s set value is assembled directly
    * from each element's scalar-written representation instead.
    */
  private[alternator] object ScalarSetFormat {
    def write[AV, T](values: Set[T])(implicit AV: AttributeValue[AV], T: ScalarDynamoFormat[T]): AV = {
      def unwrap[U](extract: AV => Option[U])(av: AV): U =
        extract(av).getOrElse(
          throw new IllegalStateException(s"ScalarDynamoFormat[$T] claims ${T.attributeType} but wrote $av")
        )

      T.attributeType match {
        case ScalarType.String =>
          import scala.jdk.CollectionConverters._
          AV.createStringSet(values.map(v => unwrap(AV.string)(T.write[AV](v))).asJavaCollection)
        case ScalarType.Numeric =>
          import scala.jdk.CollectionConverters._
          AV.createNumberSet(values.map(v => unwrap(AV.numeric)(T.write[AV](v))).asJavaCollection)
        case ScalarType.Binary =>
          AV.createByteBufferSet(values.map(v => unwrap(AV.byteBuffer)(T.write[AV](v))))
      }
    }
  }
}
