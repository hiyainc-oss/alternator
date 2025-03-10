package com.hiya.alternator

import cats.{MonadThrow, Traverse}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{AttributeValue, DynamoFormat, ScalarType, TableSchema}
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}

import scala.jdk.CollectionConverters._
import scala.collection.compat._

trait ReadScheduler[F[_]] {
  def get[V, PK](table: Table[DynamoDBClient.Missing, V, PK], key: PK)(implicit
    timeout: BatchTimeout
  ): F[Option[Result[V]]]
}

trait WriteScheduler[F[_]] {
  def put[V, PK](table: Table[DynamoDBClient.Missing, V, PK], value: V)(implicit timeout: BatchTimeout): F[Unit]
  def delete[V, PK](table: Table[DynamoDBClient.Missing, V, PK], key: PK)(implicit timeout: BatchTimeout): F[Unit]
}

sealed trait ConditionResult[+T]
object ConditionResult {
  final case class Success[T](oldValue: Option[DynamoFormat.Result[T]]) extends ConditionResult[T]
  final case object Failed extends ConditionResult[Nothing]
}

trait BatchWriteResult[Request, Response, AV] extends Any {
  def AV: AttributeValue[AV]

  def response: Response
  def unprocessed: java.util.Map[String, java.util.List[Request]]
  def unprocessedAv: Map[String, Vector[Either[java.util.Map[String, AV], java.util.Map[String, AV]]]]
  def unprocessedAvFor(table: String): Vector[Either[java.util.Map[String, AV], java.util.Map[String, AV]]]
  def unprocessedItems[V, PK](
    table: Table[_, V, PK]
  ): Vector[Either[DynamoFormat.Result[PK], DynamoFormat.Result[V]]]
}

trait BatchReadResult[Request, Response, AV] extends Any {
  def AV: AttributeValue[AV]

  def response: Response
  def processed: java.util.Map[String, java.util.List[java.util.Map[String, AV]]]
  def processedAv: Map[String, Vector[java.util.Map[String, AV]]]
  def processedAvFor(table: String): Vector[java.util.Map[String, AV]]
  def processedItems[V, PK](table: Table[_, V, PK]): Vector[DynamoFormat.Result[V]]

  def unprocessed: java.util.Map[String, Request]
  def unprocessedAv: Map[String, Vector[java.util.Map[String, AV]]]
  def unprocessedAvFor(table: String): Vector[java.util.Map[String, AV]]
  def unprocessedKeys[V, PK](table: Table[_, V, PK]): Vector[DynamoFormat.Result[PK]]
}

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

trait DynamoDBSource {
  type Client <: DynamoDBClient
  type Source[_]
  type Monad[_]

  def scan[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]] = None,
    limit: Option[Int] = None,
    consistent: Boolean = false,
    overrides: O = DynamoDBOverride.Empty
  ): Source[Result[V]]

  def query[V, PK, RK, O: DynamoDBOverride[Client, *]](
    table: TableWithRange[Client, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[Boolean]] = None,
    limit: Option[Int] = None,
    consistent: Boolean = false,
    overrides: O = DynamoDBOverride.Empty
  ): Source[Result[V]]

  def eval[T](f: => Monad[T]): Source[T]
  def evalMap[A, B](in: Source[A])(f: A => Monad[B]): Source[B]
  def bracket[T, B](acquire: => Monad[T])(release: T => Monad[Unit])(s: T => Source[B]): Source[B]
  def toSeq[T](value: Source[T]): Monad[Seq[T]]
}

object DynamoDBSource {
  type Client[S[_], C] = DynamoDBSource { type Client = C; type Source[_] = S[_] }
  type Base[S[_]] = DynamoDBSource { type Source[_] = S[_] }

  def apply[S[_]](implicit S: Base[S]): Base[S] = S
  def client[S[_], C](implicit S: Client[S, C]): Client[S, C] = S
}

abstract class DynamoDB[F[_]: MonadThrow] extends DynamoDBSource {
  override type Monad[T] = F[T]

  type AttributeValue
  type BatchReadItemRequest
  type BatchReadItemResponse
  type BatchWriteItemRequest
  type BatchWriteItemResponse

  import cats.syntax.all._

  def AV: com.hiya.alternator.schema.AttributeValue[AttributeValue]

  @inline final def get[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    pk: PK,
    consistent: Boolean = false,
    overrides: O = DynamoDBOverride.Empty
  ): F[Option[DynamoFormat.Result[V]]] =
    doGet(table, pk, consistent, overrides)

  protected def doGet[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    pk: PK,
    consistent: Boolean,
    overrides: O
  ): F[Option[Result[V]]]

  @inline final def put[V](
    table: Table[Client, V, _],
    value: V
  ): F[Unit] =
    doPut(table, value, None, DynamoDBOverride.Empty).map(_ => ())

  @inline final def put[V, O](
    table: Table[Client, V, _],
    value: V,
    condition: ConditionExpression[Boolean]
  ): F[Boolean] =
    doPut(table, value, condition = Some(condition), DynamoDBOverride.Empty)

  @inline final def put[V, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, _],
    value: V,
    overrides: O
  ): F[Unit] =
    doPut(table, value, None, overrides).map(_ => ())

  @inline final def put[V, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, _],
    value: V,
    condition: ConditionExpression[Boolean],
    overrides: O
  ): F[Boolean] =
    doPut(table, value, condition = Some(condition), overrides)

  protected def doPut[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[Boolean]

  @inline final def putAndReturn[V](table: Table[Client, V, _], value: V): F[Option[DynamoFormat.Result[V]]] =
    doPutAndReturn(table, value, None, DynamoDBOverride.Empty).flatMap {
      case ConditionResult.Success(oldValue) => MonadThrow[F].pure(oldValue)
      case ConditionResult.Failed => MonadThrow[F].raiseError(new IllegalStateException("Condition failed"))
    }

  @inline final def putAndReturn[V](
    table: Table[Client, V, _],
    value: V,
    condition: ConditionExpression[Boolean]
  ): F[ConditionResult[V]] =
    doPutAndReturn(table, value, condition = Some(condition), overrides = DynamoDBOverride.Empty)

  @inline final def putAndReturn[V, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, _],
    value: V,
    overrides: O
  ): F[Option[DynamoFormat.Result[V]]] =
    doPutAndReturn(table, value, None, overrides).flatMap {
      case ConditionResult.Success(oldValue) => MonadThrow[F].pure(oldValue)
      case ConditionResult.Failed => MonadThrow[F].raiseError(new IllegalStateException("Condition failed"))
    }

  @inline final def putAndReturn[V, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, _],
    value: V,
    condition: ConditionExpression[Boolean],
    overrides: O
  ): F[ConditionResult[V]] =
    doPutAndReturn(table, value, condition = Some(condition), overrides = overrides)

  protected def doPutAndReturn[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[ConditionResult[V]]

  @inline final def delete[V, PK, T](table: Table[Client, V, PK], key: T)(implicit T: ItemMagnet[T, V, PK]): F[Unit] =
    doDelete(table, T.key(key)(table.schema), None, DynamoDBOverride.Empty).map(_ => ())

  @inline final def delete[PK](
    table: Table[Client, _, PK],
    key: PK,
    condition: ConditionExpression[Boolean]
  ): F[Boolean] =
    doDelete(table, key, Some(condition), DynamoDBOverride.Empty)

  @inline final def delete[V, PK, T, O: DynamoDBOverride[Client, *]](table: Table[Client, V, PK], key: T, overrides: O)(
    implicit T: ItemMagnet[T, V, PK]
  ): F[Unit] =
    doDelete(table, T.key(key)(table.schema), None, overrides).map(_ => ())

  @inline final def delete[PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, _, PK],
    key: PK,
    condition: ConditionExpression[Boolean],
    overrides: O
  ): F[Boolean] =
    doDelete(table, key, Some(condition), overrides)

  protected def doDelete[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[Boolean]

  @inline final def deleteAndReturn[T, V, PK](table: Table[Client, V, PK], key: T)(implicit
    T: ItemMagnet[T, V, PK]
  ): F[Option[DynamoFormat.Result[V]]] =
    doDeleteAndReturn(table, T.key(key)(table.schema), None, DynamoDBOverride.Empty).flatMap {
      case ConditionResult.Success(oldValue) => MonadThrow[F].pure(oldValue)
      case ConditionResult.Failed => MonadThrow[F].raiseError(new IllegalStateException("Condition failed"))
    }

  @inline final def deleteAndReturn[T, V, PK](
    table: Table[Client, V, PK],
    key: T,
    condition: ConditionExpression[Boolean]
  )(implicit T: ItemMagnet[T, V, PK]): F[ConditionResult[V]] =
    doDeleteAndReturn(table, T.key(key)(table.schema), Some(condition), DynamoDBOverride.Empty)

  @inline final def deleteAndReturn[T, V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    key: T,
    overrides: O
  )(implicit
    T: ItemMagnet[T, V, PK]
  ): F[Option[DynamoFormat.Result[V]]] =
    doDeleteAndReturn(table, T.key(key)(table.schema), None, overrides).flatMap {
      case ConditionResult.Success(oldValue) => MonadThrow[F].pure(oldValue)
      case ConditionResult.Failed => MonadThrow[F].raiseError(new IllegalStateException("Condition failed"))
    }

  @inline final def deleteAndReturn[T, V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Client, V, PK],
    key: T,
    condition: ConditionExpression[Boolean],
    overrides: O
  )(implicit T: ItemMagnet[T, V, PK]): F[ConditionResult[V]] =
    doDeleteAndReturn(table, T.key(key)(table.schema), Some(condition), overrides)

  protected def doDeleteAndReturn[V, PK, O: DynamoDBOverride[Client, *]](
    value: Table[Client, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[ConditionResult[V]]

  @inline final def batchWrite(
    client: Client,
    values: Map[String, Seq[BatchWriteItemRequest]]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]] =
    batchWrite(client, values.view.mapValues(_.asJava).toMap.asJava)

  def batchWrite(
    client: Client,
    values: java.util.Map[String, java.util.List[BatchWriteItemRequest]]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]]

  @inline final def batchPutAll[V, PK](
    table: Table[Client, V, PK],
    values: Seq[V]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]] =
    batchWrite(table.client, Map(table.tableName -> values.map(batchPutRequest(table, _))))

  @inline final def batchDeleteAll[T, V, PK](table: Table[Client, V, PK], keys: Seq[T])(implicit
    T: ItemMagnet[T, V, PK]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]] =
    batchWrite(
      table.client,
      Map(table.tableName -> keys.map(key => batchDeleteRequest(table, T.key(key)(table.schema))))
    )

  @inline final def batchGetAll[V, PK](
    table: Table[Client, V, PK],
    keys: Seq[PK]
  ): F[BatchReadResult[BatchReadItemRequest, BatchReadItemResponse, AttributeValue]] =
    batchGet(table.client, Map(table.tableName -> keys.map(batchGetRequest(table, _))))

  def batchGet(
    client: Client,
    keys: Map[String, Seq[java.util.Map[String, AttributeValue]]]
  ): F[BatchReadResult[BatchReadItemRequest, BatchReadItemResponse, AttributeValue]]

  def batchGet(
    client: Client,
    keys: java.util.Map[String, BatchReadItemRequest]
  ): F[BatchReadResult[BatchReadItemRequest, BatchReadItemResponse, AttributeValue]]

  def batchPutRequest[V, PK](table: Table[Client, V, PK], value: V): BatchWriteItemRequest
  def batchDeleteRequest[V, PK](table: Table[Client, V, PK], key: PK): BatchWriteItemRequest
  def batchGetRequest[V, PK](table: Table[Client, V, PK], key: PK): java.util.Map[String, AttributeValue]

  def createTable[O: DynamoDBOverride[Client, *]](
    client: Client,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String] = None,
    readCapacity: Long = 1L,
    writeCapacity: Long = 1L,
    attributes: List[(String, ScalarType)] = Nil,
    overrides: O = DynamoDBOverride.Empty
  ): F[Unit]

  def dropTable[O: DynamoDBOverride[Client, *]](
    client: Client,
    tableName: String,
    overrides: O = DynamoDBOverride.Empty
  ): F[Unit]

  def isRetryable(e: Throwable): Boolean
  def isThrottling(e: Throwable): Boolean
  def parTraverse[M[_]: Traverse, A, B](values: M[A])(f: A => F[B]): F[M[B]]

}

object DynamoDB {
  def apply[F[_]](implicit D: DynamoDB[F]): DynamoDB[F] = D
  def client[F[_], C](implicit D: Client[F, C]): Client[F, C] = D

  type Source[S[_]] = DynamoDBSource.Base[S]
  type SourceClient[S[_], C] = DynamoDBSource.Client[S, C]

  type Aux[F[_], S[_], C] = DynamoDB[F] {
    type Source[T] = S[T]
    type Client = C
  }

  type Client[F[_], C] = DynamoDB[F] {
    type Client = C
  }

  type Types[F[_], T] = DynamoDB[F] {
    type Types = T
  }

}
