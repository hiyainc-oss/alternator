package com.hiya.alternator

import cats.free.Free.liftF
import com.hiya.alternator.schema.{DynamoAttributeError, DynamoFormat, TableSchema, TableSchemaWithRange}
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}

import scala.util.{Failure, Success, Try}

package internal {
  sealed trait DynamoDBA[A]
  final case class Get[C, V, PK](table: TableLike[C, V, PK], key: PK) extends DynamoDBA[Option[DynamoFormat.Result[V]]]
  final case class Put[C, V, PK](table: TableLike[C, V, PK], value: V, condition: Option[ConditionExpression[Boolean]])
    extends DynamoDBA[Boolean]
  final case class Delete[C, V, PK](
    table: TableLike[C, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ) extends DynamoDBA[Boolean]

  sealed trait DynamoDBSourceA[A]
  final case class Scan[C, V, PK](value: TableLike[C, V, PK], segment: Option[Segment])
    extends DynamoDBSourceA[DynamoFormat.Result[V]]
  final case class Query[C, V, PK, RK](value: TableWithRangeLike[C, V, PK, RK], pk: PK, rk: RKCondition[RK])
    extends DynamoDBSourceA[DynamoFormat.Result[V]]
}

trait Client[DB[_], C]

object Client {
  sealed trait Missing
  object Missing extends Missing
}

abstract class TableLike[C, V, PK](
  val client: C,
  val name: String
) {
  def schema: TableSchema.Aux[V, PK]

  def get(pk: PK): DynamoDB[Option[DynamoFormat.Result[V]]] =
    liftF[internal.DynamoDBA, Option[DynamoFormat.Result[V]]](internal.Get(this, pk))

  def put(value: V): DynamoDB[Unit] =
    liftF[internal.DynamoDBA, Boolean](internal.Put(this, value, None)).map(_ => ())

  def put(value: V, condition: ConditionExpression[Boolean]): DynamoDB[Boolean] =
    liftF[internal.DynamoDBA, Boolean](internal.Put(this, value, condition = Some(condition)))

  def delete(key: PK): DynamoDB[Unit] =
    liftF[internal.DynamoDBA, Boolean](internal.Delete(this, key, None)).map(_ => ())

  def delete(key: PK, condition: ConditionExpression[Boolean]): DynamoDB[Boolean] =
    liftF[internal.DynamoDBA, Boolean](internal.Delete(this, key, Some(condition)))

  def scan(segment: Option[Segment] = None): DynamoDBSource[DynamoFormat.Result[V]] =
    liftF[internal.DynamoDBSourceA, DynamoFormat.Result[V]](internal.Scan(this, segment))

//  def batchGet[DB[_]: DynamoDB](values: Seq[PK]): DB[BatchGetItemResponse]
//  def batchPut[DB[_]: DynamoDB](values: Seq[V]): DB[BatchWriteItemResponse]
//  def batchDelete[DB[_]: DynamoDB, T](values: Seq[T])(implicit T: ItemMagnet[T, V, PK]): Future[BatchWriteItemResponse]
//  def batchWrite[DB[_]: DynamoDB](values: Seq[Either[PK, V]]): Future[BatchWriteItemResponse]
}

abstract class TableWithRangeLike[C, V, PK, RK](c: C, name: String) extends TableLike[C, V, (PK, RK)](c, name) {
  override def schema: TableSchemaWithRange.Aux[V, PK, RK]

  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): DynamoDBSource[DynamoFormat.Result[V]] =
    liftF[internal.DynamoDBSourceA, DynamoFormat.Result[V]](internal.Query(this, pk, rk))

}

class Table[V, PK](
  name: String,
  override val schema: TableSchema.Aux[V, PK]
) extends TableLike[Client.Missing, V, PK](Client.Missing, name)

class TableWithRangeKey[V, PK, RK](
  name: String,
  override val schema: TableSchemaWithRange.Aux[V, PK, RK]
) extends TableWithRangeLike[Client.Missing, V, PK, RK](Client.Missing, name)

object Table {
  final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

  final def orFail[T](x: DynamoFormat.Result[T]): Try[T] = x match {
    case Left(error) => Failure(DynamoDBException(error))
    case Right(value) => Success(value)
  }

  def tableWithPK[V](name: String)(implicit
    tableSchema: TableSchema[V]
  ): Table[V, tableSchema.IndexType] =
    new Table[V, tableSchema.IndexType](name, tableSchema)

  def tableWithRK[V](name: String)(implicit
    tableSchema: TableSchemaWithRange[V]
  ): TableWithRangeKey[V, tableSchema.PK, tableSchema.RK] =
    new TableWithRangeKey[V, tableSchema.PK, tableSchema.RK](name, tableSchema)

}
