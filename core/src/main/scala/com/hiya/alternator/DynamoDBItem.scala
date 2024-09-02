package com.hiya.alternator

import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition, Segment}

trait DynamoDBItem[F[_], C] {
  type BatchGetItemResponse
  type BatchWriteItemResponse

  def get[V, PK](table: TableLike[C, V, PK], pk: PK): F[Option[Result[V]]]
  def put[V, PK](table: TableLike[C, V, PK], item: V, condition: Option[ConditionExpression[Boolean]]): F[Boolean]
  def delete[V, PK](table: TableLike[C, V, PK], key: PK, condition: Option[ConditionExpression[Boolean]]): F[Boolean]
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
  def batchGet[V, PK](table: TableLike[C, V, PK], keys: Seq[PK]): F[BatchGetItemResponse]
  def batchWrite[V, PK](table: TableLike[C, V, PK], values: Seq[Either[PK, V]]): F[BatchWriteItemResponse]
}

object DynamoDBItem {
  def apply[F[_], C](implicit D: DynamoDBItem[F, C]): DynamoDBItem[F, C] = D
}

trait DynamoDBSource[F[_], C] {
  def scan[V, PK](table: TableLike[C, V, PK], segment: Option[Segment]): F[Result[V]]
  def query[V, PK, RK](table: TableWithRangeKeyLike[C, V, PK, RK], pk: PK, rk: RKCondition[RK]): F[Result[V]]
}

object DynamoDBSource {
  def apply[F[_], C](implicit D: DynamoDBSource[F, C]): DynamoDBSource[F, C] = D
}

trait DynamoDB[F[_], S[_], C] extends DynamoDBItem[F, C] with DynamoDBSource[S, C] {
  def eval[T](f: => F[T]): S[T]
  def evalMap[A, B](in: S[A])(f: A => F[B]): S[B]
  def bracket[T, B](acquire: => F[T])(release: T => F[Unit])(s: T => S[B]): S[B]
}

object DynamoDB {
  def apply[F[_], S[_], C](implicit D: DynamoDB[F, S, C]): DynamoDB[F, S, C] = D
}
