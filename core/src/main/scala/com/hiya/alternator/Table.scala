package com.hiya.alternator

import cats.syntax.all._
import cats.{Functor, MonadThrow}
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

sealed trait ConditionResult[+T]
object ConditionResult {
  final case class Success[T](oldValue: Option[DynamoFormat.Result[T]]) extends ConditionResult[T]
  final case object Failed extends ConditionResult[Nothing]
}

abstract class TableLike[C, V, PK](
  val client: C,
  val tableName: String
) {

  def withClient[C1](client: C1): TableLike[C1, V, PK] = new TableLike[C1, V, PK](client, tableName) {
    override def schema: TableSchema.Aux[V, PK] = TableLike.this.schema
  }

  def schema: TableSchema.Aux[V, PK]

  def get[F[_]](pk: PK)(implicit DB: DynamoDBItem[F, C]): F[Option[DynamoFormat.Result[V]]] =
    DB.get(this, pk)

  def put[F[_]: Functor](value: V)(implicit DB: DynamoDBItem[F, C]): F[Unit] =
    DB.put(this, value, None).map(_ => ())

  def putAndReturn[F[_]: MonadThrow](value: V)(implicit DB: DynamoDBItem[F, C]): F[Option[DynamoFormat.Result[V]]] =
    DB.putAndReturn(this, value, None).flatMap {
      case ConditionResult.Success(oldValue) => oldValue.pure[F]
      case ConditionResult.Failed => MonadThrow[F].raiseError(new IllegalStateException("Condition failed"))
    }

  def put[F[_]](value: V, condition: ConditionExpression[Boolean])(implicit DB: DynamoDBItem[F, C]): F[Boolean] =
    DB.put(this, value, condition = Some(condition))

  def putAndReturn[F[_]](value: V, condition: ConditionExpression[Boolean])(implicit
    DB: DynamoDBItem[F, C]
  ): F[ConditionResult[V]] =
    DB.putAndReturn(this, value, condition = Some(condition))

  def delete[F[_]: Functor](key: PK)(implicit DB: DynamoDBItem[F, C]): F[Unit] =
    DB.delete(this, key, None).map(_ => ())

  def deleteAndReturn[F[_]: MonadThrow](key: PK)(implicit DB: DynamoDBItem[F, C]): F[Option[DynamoFormat.Result[V]]] =
    DB.deleteAndReturn(this, key, None).flatMap {
      case ConditionResult.Success(oldValue) => oldValue.pure[F]
      case ConditionResult.Failed => MonadThrow[F].raiseError(new IllegalStateException("Condition failed"))
    }

  def delete[F[_]](key: PK, condition: ConditionExpression[Boolean])(implicit DB: DynamoDBItem[F, C]): F[Boolean] =
    DB.delete(this, key, Some(condition))

  def deleteAndReturn[F[_]](key: PK, condition: ConditionExpression[Boolean])(implicit
    DB: DynamoDBItem[F, C]
  ): F[ConditionResult[V]] =
    DB.deleteAndReturn(this, key, Some(condition))

  def scan[F[_]](segment: Option[Segment] = None, condition: Option[ConditionExpression[Boolean]] = None)(implicit
    DB: DynamoDBSource[F, C]
  ): F[DynamoFormat.Result[V]] =
    DB.scan(this, segment, condition)

  def batchGetRequest[F[_]](key: PK)(implicit DB: DynamoDBItem[F, C]): java.util.Map[String, DB.AttributeValue] =
    DB.batchGetRequest(this, key)

  def batchPutAll[F[_]](values: Seq[V])(implicit
    DB: DynamoDBItem[F, C]
  ): F[BatchWriteResult[DB.BatchWriteItemRequest, DB.BatchWriteItemResponse, DB.AttributeValue]] =
    DB.batchWrite(client, Map(tableName -> values.map(batchPutRequest(_)(DB))))

  def batchPutRequest[F[_]](value: V)(implicit DB: DynamoDBItem[F, C]): DB.BatchWriteItemRequest =
    DB.batchPutRequest(this, value)

  def batchDeleteAll[F[_], T](keys: Seq[T])(implicit
    T: ItemMagnet[T, V, PK],
    DB: DynamoDBItem[F, C]
  ): F[BatchWriteResult[DB.BatchWriteItemRequest, DB.BatchWriteItemResponse, DB.AttributeValue]] =
    DB.batchWrite(client, Map(tableName -> keys.map(key => batchDeleteRequest(T.key(key)(this.schema))(DB))))

  def batchDeleteRequest[F[_]](key: PK)(implicit DB: DynamoDBItem[F, C]): DB.BatchWriteItemRequest =
    DB.batchDeleteRequest(this, key)

  def batchedPut[F[_]](value: V)(implicit batchReader: WriteScheduler[C, F], timeout: BatchTimeout): F[Unit] =
    batchReader.put(this, value)

  def batchedGet[F[_]](
    key: PK
  )(implicit batchReader: ReadScheduler[C, F], timeout: BatchTimeout): F[Option[DynamoFormat.Result[V]]] =
    batchReader.get(this, key)

  def batchedDelete[F[_]](key: PK)(implicit batchReader: WriteScheduler[C, F], timeout: BatchTimeout): F[Unit] =
    batchReader.delete(this, key)
}

abstract class TableWithRangeKeyLike[C, V, PK, RK](c: C, name: String) extends TableLike[C, V, (PK, RK)](c, name) {
  override def withClient[C1](client: C1): TableWithRangeKeyLike[C1, V, PK, RK] =
    new TableWithRangeKeyLike[C1, V, PK, RK](client, name) {
      override def schema: TableSchemaWithRange.Aux[V, PK, RK] = TableWithRangeKeyLike.this.schema
    }

  override def schema: TableSchemaWithRange.Aux[V, PK, RK]

  def query[F[_]](
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[Boolean]] = None
  )(implicit
    DB: DynamoDBSource[F, C]
  ): F[DynamoFormat.Result[V]] =
    DB.query(this, pk, rk, condition)
}

class Table[V, PK](
  name: String,
  override val schema: TableSchema.Aux[V, PK]
) extends TableLike[Client.Missing, V, PK](Client.Missing, name)

class TableWithRangeKey[V, PK, RK](
  name: String,
  override val schema: TableSchemaWithRange.Aux[V, PK, RK]
) extends TableWithRangeKeyLike[Client.Missing, V, PK, RK](Client.Missing, name)

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
  )(implicit DB: DynamoDBItem[F, C]): F[Unit] =
    DB.createTable(
      client,
      tableName,
      hashKey,
      rangeKey,
      readCapacity,
      writeCapacity,
      attributes
    )

  def drop[F[_], C](tableName: String, client: C = Client.Missing)(implicit DB: DynamoDBItem[F, C]): F[Unit] =
    DB.dropTable(client, tableName)
}
