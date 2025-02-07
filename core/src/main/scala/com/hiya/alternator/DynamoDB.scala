package com.hiya.alternator

import cats.Traverse
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{AttributeValue, DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}

import scala.jdk.CollectionConverters._
import scala.collection.compat._

trait BatchWriteResult[Request, Response, AV] extends Any {
  def AV: AttributeValue[AV]

  def response: Response
  def unprocessed: java.util.Map[String, java.util.List[Request]]
  def unprocessedAv: Map[String, Vector[Either[java.util.Map[String, AV], java.util.Map[String, AV]]]]
  def unprocessedAvFor(table: String): Vector[Either[java.util.Map[String, AV], java.util.Map[String, AV]]]
  def unprocessedItems[V, PK](
    table: TableLike[_, V, PK]
  ): Vector[Either[DynamoFormat.Result[PK], DynamoFormat.Result[V]]]
}

trait BatchReadResult[Request, Response, AV] extends Any {
  def AV: AttributeValue[AV]

  def response: Response
  def processed: java.util.Map[String, java.util.List[java.util.Map[String, AV]]]
  def processedAv: Map[String, Vector[java.util.Map[String, AV]]]
  def processedAvFor(table: String): Vector[java.util.Map[String, AV]]
  def processedItems[V, PK](table: TableLike[_, V, PK]): Vector[DynamoFormat.Result[V]]

  def unprocessed: java.util.Map[String, Request]
  def unprocessedAv: Map[String, Vector[java.util.Map[String, AV]]]
  def unprocessedAvFor(table: String): Vector[java.util.Map[String, AV]]
  def unprocessedKeys[V, PK](table: TableLike[_, V, PK]): Vector[DynamoFormat.Result[PK]]
}

trait DynamoDBSourceBase {
  type C
  type S[_]

  def scan[V, PK](
    table: TableLike[C, V, PK],
    segment: Option[Segment],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int] = None,
    consistent: Boolean = false
  ): S[Result[V]]

  def query[V, PK, RK](
    table: TableWithRangeKeyLike[C, V, PK, RK],
    pk: PK,
    rk: RKCondition[RK],
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int] = None,
    consistent: Boolean = false
  ): S[Result[V]]

}

object DynamoDBSource {
  type Aux[PS[_], PC] = DynamoDBSourceBase {
    type S[T] = PS[T]
    type C = PC
  }
}

trait DynamoDB[F[_]] extends DynamoDBSourceBase {
  type AttributeValue
  type BatchReadItemRequest
  type BatchReadItemResponse
  type BatchWriteItemRequest
  type BatchWriteItemResponse
  type Overrides

  def AV: com.hiya.alternator.schema.AttributeValue[AttributeValue]

  def get[V, PK](
    table: TableLike[C, V, PK],
    pk: PK,
    consistent: Boolean = false,
    overrides: Option[Overrides] = None
  ): F[Option[Result[V]]]
  def put[V, PK](
    table: TableLike[C, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]] = None,
    overrides: Option[Overrides] = None
  ): F[Boolean]
  def putAndReturn[V, PK](
    table: TableLike[C, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]] = None,
    overrides: Option[Overrides] = None
  ): F[ConditionResult[V]]
  def delete[V, PK](
    table: TableLike[C, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]] = None,
    overrides: Option[Overrides] = None
  ): F[Boolean]
  def deleteAndReturn[V, PK](
    value: TableLike[C, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]] = None,
    overrides: Option[Overrides] = None
  ): F[ConditionResult[V]]
  def createTable(
    client: C,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)]
  ): F[Unit]
  def dropTable(client: C, tableName: String): F[Unit]

  def batchPutRequest[V, PK](
    table: TableLike[C, V, PK],
    value: V
  ): BatchWriteItemRequest
  def batchDeleteRequest[V, PK](
    table: TableLike[C, V, PK],
    key: PK
  ): BatchWriteItemRequest
  def batchWrite(
    client: C,
    values: Map[String, Seq[BatchWriteItemRequest]]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]] =
    batchWrite(client, values, None)
  def batchWrite(
    client: C,
    values: Map[String, Seq[BatchWriteItemRequest]],
    overrides: Option[Overrides]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]] =
    batchWrite(client, values.view.mapValues(_.asJava).toMap.asJava, overrides)
  def batchWrite(
    client: C,
    values: java.util.Map[String, java.util.List[BatchWriteItemRequest]],
    overrides: Option[Overrides] = None
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]]

  def batchGetRequest[V, PK](
    table: TableLike[C, V, PK],
    key: PK
  ): java.util.Map[String, AttributeValue]
  def batchGetAV(
    client: C,
    keys: Map[String, Seq[java.util.Map[String, AttributeValue]]],
    overrides: Option[Overrides] = None
  ): F[BatchReadResult[BatchReadItemRequest, BatchReadItemResponse, AttributeValue]]
  def batchGet(
    client: C,
    keys: java.util.Map[String, BatchReadItemRequest],
    overrides: Option[Overrides] = None
  ): F[BatchReadResult[BatchReadItemRequest, BatchReadItemResponse, AttributeValue]]

  def isRetryable(e: Throwable): Boolean
  def isThrottling(e: Throwable): Boolean
  def parTraverse[M[_]: Traverse, A, B](values: M[A])(f: A => F[B]): F[M[B]]

  def eval[T](f: => F[T]): S[T]
  def evalMap[A, B](in: S[A])(f: A => F[B]): S[B]
  def bracket[T, B](acquire: => F[T])(release: T => F[Unit])(s: T => S[B]): S[B]
  def toSeq[T](value: S[T]): F[Seq[T]]
}

object DynamoDB {
  def apply[F[_]](implicit D: DynamoDB[F]): DynamoDB[F] = D

  type Aux[F[_], PS[_], PC] = DynamoDB[F] {
    type S[T] = PS[T]
    type C = PC
  }

  type Client[F[_], PC] = DynamoDB[F] {
    type C = PC
  }

  type Source[PS[_]] = DynamoDBSourceBase {
    type S[T] = PS[T]
  }
}
