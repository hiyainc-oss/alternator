package com.hiya.alternator.aws2.internal

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator._
import com.hiya.alternator.aws2._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.ConditionExpression
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemResponse,
  BatchWriteItemRequest,
  KeysAndAttributes,
  WriteRequest
}
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

import java.util
import java.util.concurrent.CompletionException
import scala.jdk.CollectionConverters._
import scala.collection.compat._

abstract class Aws2DynamoDB[F[+_]: MonadThrow, S[_]] extends DynamoDB[F] {
  override type Client = DynamoDbAsyncClient
  override type Source[T] = S[T]

  override type AttributeValue = model.AttributeValue
  override type BatchReadItemRequest = KeysAndAttributes
  override type BatchReadItemResponse = model.BatchGetItemResponse
  override type BatchWriteItemRequest = model.WriteRequest
  override type BatchWriteItemResponse = model.BatchWriteItemResponse

  override def AV: schema.AttributeValue[AttributeValue] = Aws2IsAttributeValues

  protected def async[T](f: => java.util.concurrent.CompletableFuture[T]): F[T]

  override def isRetryable(e: Throwable): Boolean = Exceptions.isRetryable(e)
  override def isThrottling(e: Throwable): Boolean = Exceptions.isThrottle(e)

  override protected def doGet[V, PK](
    table: Table[DynamoDbAsyncClient, V, PK],
    pk: PK,
    consistent: Boolean
  ): F[Option[Result[V]]] = {
    async(table.client.getItem(Aws2TableOps(table).get(pk, consistent).build())).map(Aws2TableOps(table).deserialize)
  }

  override protected def doPut[V, PK](
    table: Table[DynamoDbAsyncClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(table.client.putItem(Aws2TableOps(table).put(item, condition).build()))
          .map(_ => true)
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        async(table.client.putItem(Aws2TableOps(table).put(item, returnOld = false).build()))
          .map(_ => true)
    }

  override protected def doPutAndReturn[V, PK](
    table: Table[DynamoDbAsyncClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]]
  ): F[ConditionResult[V]] = {
    val req = condition match {
      case Some(condition) =>
        Aws2TableOps(table).put(item, condition, returnOld = true).build()
      case None =>
        Aws2TableOps(table).put(item, returnOld = true).build()
    }

    async(table.client.putItem(req))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws2TableOps(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override protected def doDelete[V, PK](
    table: Table[DynamoDbAsyncClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(table.client.deleteItem(Aws2TableOps(table).delete(key, condition).build()))
          .map(_ => true)
          .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        async(table.client.deleteItem(Aws2TableOps(table).delete(key).build()))
          .map(_ => true)
    }

  override protected def doDeleteAndReturn[V, PK](
    table: Table[DynamoDbAsyncClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ): F[ConditionResult[V]] = {
    val req = condition match {
      case Some(condition) =>
        Aws2TableOps(table).delete(key, condition, returnOld = true).build()
      case None =>
        Aws2TableOps(table).delete(key, returnOld = true).build()
    }

    async(table.client.deleteItem(req))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws2TableOps(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override def createTable(
    client: DynamoDbAsyncClient,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)]
  ): F[Unit] =
    async(
      client
        .createTable(
          Aws2TableOps.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes).build()
        )
    ).map(_ => ())

  override def dropTable(client: DynamoDbAsyncClient, tableName: String): F[Unit] =
    async(client.deleteTable(model.DeleteTableRequest.builder().tableName(tableName).build())).map(_ => ())

  override def batchGetRequest[V, PK](
    table: Table[DynamoDbAsyncClient, V, PK],
    key: PK
  ): java.util.Map[String, AttributeValue] =
    table.schema.serializePK(key)

  override def batchGet(
    client: DynamoDbAsyncClient,
    keys: Map[String, Seq[util.Map[String, AttributeValue]]]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue]] =
    batchGet(client, keys.view.mapValues({ kv => KeysAndAttributes.builder().keys(kv.asJava).build() }).toMap.asJava)

  override def batchGet(
    client: DynamoDbAsyncClient,
    keys: util.Map[String, KeysAndAttributes]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue]] =
    async(client.batchGetItem(model.BatchGetItemRequest.builder().requestItems(keys).build()))
      .map(Aws2BatchRead(_))

  override def batchPutRequest[V, PK](table: Table[DynamoDbAsyncClient, V, PK], value: V): WriteRequest =
    WriteRequest.builder().putRequest(Aws2TableOps(table).putRequest(value).build()).build()

  override def batchDeleteRequest[V, PK](table: Table[DynamoDbAsyncClient, V, PK], key: PK): WriteRequest =
    WriteRequest.builder().deleteRequest(Aws2TableOps(table).deleteRequest(key).build()).build()

  override def batchWrite(
    client: DynamoDbAsyncClient,
    values: util.Map[String, util.List[WriteRequest]]
  ): F[Aws2BatchWrite] =
    async(client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(values).build()))
      .map(Aws2BatchWrite(_))
}
