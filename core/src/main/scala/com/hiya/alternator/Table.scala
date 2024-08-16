package com.hiya.alternator

import cats.Functor
import cats.syntax.all._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema._
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}

import scala.util.{Failure, Success, Try}

sealed trait ItemMagnet[T, V, PK] {
  def key(t: T)(implicit schema: TableSchema.Aux[V, PK]): PK
}

object ItemMagnet {
  implicit def whole[V, PK]: ItemMagnet[V, V, PK] = new ItemMagnet[V, V, PK] {
    override def key(t: V)(implicit schema: TableSchema.Aux[V, PK]): PK = schema.extract(t)
  }

  implicit def itemKey[V, PK]: ItemMagnet[PK, V, PK] = new ItemMagnet[PK, V, PK] {
    override def key(t: PK)(implicit schema: TableSchema.Aux[V, PK]): PK = t
  }
}

trait Client[DB[_], C]

object Client {
  sealed trait Missing
  object Missing extends Missing
}

trait ReadScheduler[C, F[_]] {
  def get[V, PK](table: TableLike[C, V, PK], key: PK)(implicit timeout: BatchTimeout): F[Option[Result[V]]]
}

trait WriteScheduler[C, F[_]] {
  def put[V, PK](table: TableLike[C, V, PK], value: V)(implicit timeout: BatchTimeout): F[Unit]
  def delete[V, PK](table: TableLike[C, V, PK], key: PK)(implicit timeout: BatchTimeout): F[Unit]
}

abstract class TableLike[C, V, PK](
  val client: C,
  val tableName: String
) {

  def withClient[C1](client: C1): TableLike[C1, V, PK] = new TableLike[C1, V, PK](client, tableName) {
    override def schema: TableSchema.Aux[V, PK] = TableLike.this.schema
  }

  def schema: TableSchema.Aux[V, PK]

  def get[F[_]](pk: PK)(implicit DB: DynamoDBValue[F, C]): F[Option[DynamoFormat.Result[V]]] =
    DB.get(this, pk)

  def put[F[_]: Functor](value: V)(implicit DB: DynamoDBValue[F, C]): F[Unit] =
    DB.put(this, value, None).map(_ => ())

  def put[F[_]](value: V, condition: ConditionExpression[Boolean])(implicit DB: DynamoDBValue[F, C]): F[Boolean] =
    DB.put(this, value, condition = Some(condition))

  def delete[F[_]: Functor](key: PK)(implicit DB: DynamoDBValue[F, C]): F[Unit] =
    DB.delete(this, key, None).map(_ => ())

  def delete[F[_]](key: PK, condition: ConditionExpression[Boolean])(implicit DB: DynamoDBValue[F, C]): F[Boolean] =
    DB.delete(this, key, Some(condition))

  def scan[F[_]](segment: Option[Segment] = None)(implicit DB: DynamoDBSource[F, C]): F[DynamoFormat.Result[V]] =
    DB.scan(this, segment)

  def batchGet[F[_]](keys: Seq[PK])(implicit DB: DynamoDBValue[F, C]): F[DB.BatchGetItemResponse] =
    DB.batchGet(this, keys)

  def batchPut[F[_]](values: Seq[V])(implicit DB: DynamoDBValue[F, C]): F[DB.BatchWriteItemResponse] =
    batchWrite(values.map(Right(_)))

  def batchDelete[F[_], T](
    keys: Seq[T]
  )(implicit T: ItemMagnet[T, V, PK], DB: DynamoDBValue[F, C]): F[DB.BatchWriteItemResponse] =
    batchWrite(keys.map(x => Left(T.key(x)(schema))))

  def batchWrite[F[_]](values: Seq[Either[PK, V]])(implicit DB: DynamoDBValue[F, C]): F[DB.BatchWriteItemResponse] =
    DB.batchWrite(this, values)

  def batchedGet[F[_]](
    key: PK
  )(implicit batchReader: ReadScheduler[C, F], timeout: BatchTimeout): F[Option[DynamoFormat.Result[V]]] =
    batchReader.get(this, key)

  def batchedPut[F[_]](value: V)(implicit batchReader: WriteScheduler[C, F], timeout: BatchTimeout): F[Unit] =
    batchReader.put(this, value)

  def batchedDelete[F[_]](key: PK)(implicit batchReader: WriteScheduler[C, F], timeout: BatchTimeout): F[Unit] =
    batchReader.delete(this, key)
}

abstract class TableWithRangeLike[C, V, PK, RK](c: C, name: String) extends TableLike[C, V, (PK, RK)](c, name) {
  override def withClient[C1](client: C1): TableWithRangeLike[C1, V, PK, RK] =
    new TableWithRangeLike[C1, V, PK, RK](client, name) {
      override def schema: TableSchemaWithRange.Aux[V, PK, RK] = TableWithRangeLike.this.schema
    }

  override def schema: TableSchemaWithRange.Aux[V, PK, RK]

  def query[F[_]](pk: PK, rk: RKCondition[RK] = RKCondition.empty)(implicit
    DB: DynamoDBSource[F, C]
  ): F[DynamoFormat.Result[V]] =
    DB.query(this, pk, rk)

}

class Table[V, PK](
  name: String,
  override val schema: TableSchema.Aux[V, PK]
) extends TableLike[Client.Missing, V, PK](Client.Missing, name)

class TableWithRangeKey[V, PK, RK](
  name: String,
  override val schema: TableSchemaWithRange.Aux[V, PK, RK]
) extends TableWithRangeLike[Client.Missing, V, PK, RK](Client.Missing, name)

final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

object Table {
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

  def create[F[_], C](
    tableName: String,
    hashKey: String,
    rangeKey: Option[String] = None,
    readCapacity: Long = 1L,
    writeCapacity: Long = 1L,
    attributes: List[(String, ScalarType)] = Nil,
    client: C = Client.Missing
  )(implicit DB: DynamoDBValue[F, C]): F[Unit] =
    DB.createTable(
      client,
      tableName,
      hashKey,
      rangeKey,
      readCapacity,
      writeCapacity,
      attributes
    )

  def drop[F[_], C](tableName: String, client: C = Client.Missing)(implicit DB: DynamoDBValue[F, C]): F[Unit] =
    DB.dropTable(client, tableName)
}
