package com.hiya.alternator.crud

import com.hiya.alternator.syntax.RKCondition
import com.hiya.alternator.{DynamoFormat, Segment, Table}
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse

trait TableOps[V, PK, Future[_], Source[_]] {
  val table: Table[V, PK]

  def get(pk: PK): Future[Option[DynamoFormat.Result[V]]]
  def put(value: V): Future[Unit]
  def delete(key: PK): Future[Unit]
  def scan(segment: Option[Segment] = None): Source[DynamoFormat.Result[V]]

  def batchGet(values: Seq[V]): Future[BatchWriteItemResponse]
  def batchPut(values: Seq[V]): Future[BatchWriteItemResponse]
  def batchDelete[T : table.ItemMagnet](values: Seq[T]): Future[BatchWriteItemResponse]
  def batchWrite(values: Seq[Either[PK, V]]): Future[BatchWriteItemResponse]
}

trait TableWithRangeOps[V, PK, RK, F[_], S[_]] extends TableOps[V, (PK, RK), F, S] {
  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): S[DynamoFormat.Result[V]]
}