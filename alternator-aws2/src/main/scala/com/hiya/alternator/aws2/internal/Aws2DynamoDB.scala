package com.hiya.alternator.aws2.internal

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator.DynamoDBOverride.OverrideOps
import com.hiya.alternator._
import com.hiya.alternator.aws2._
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.ConditionExpression
import software.amazon.awssdk.services.dynamodb.model
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemResponse,
  BatchWriteItemRequest,
  KeysAndAttributes,
  WriteRequest
}

import java.util
import java.util.concurrent.CompletionException
import scala.collection.compat._
import scala.jdk.CollectionConverters._

abstract class Aws2DynamoDB[F[+_]: MonadThrow, S[_]] extends DynamoDB[F] {
  override type Client = Aws2DynamoDBClient
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

  override protected def doGet[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Aws2DynamoDBClient, V, PK],
    pk: PK,
    consistent: Boolean,
    overrides: O
  ): F[Option[Result[V]]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    async(table.client.underlying.getItem(Aws2TableOps(table).get(pk, consistent, resolvedOverride).build()))
      .map(Aws2TableOps(table).deserialize)
  }

  override protected def doPut[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Aws2DynamoDBClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[Boolean] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    val req = Aws2TableOps(table).put(item, condition, returnOld = false, overrides = resolvedOverride).build()
    async(table.client.underlying.putItem(req))
      .map(_ => true)
      .optApp[ConditionExpression[Boolean]](f =>
        _ => f.recover { case _: model.ConditionalCheckFailedException => false }
      )(condition)
  }

  override protected def doPutAndReturn[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Aws2DynamoDBClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[ConditionResult[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    val req = Aws2TableOps(table).put(item, condition, returnOld = true, overrides = resolvedOverride).build()

    async(table.client.underlying.putItem(req))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws2TableOps(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override protected def doDelete[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Aws2DynamoDBClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[Boolean] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    val req = Aws2TableOps(table).delete(key, condition, returnOld = false, resolvedOverride).build()
    async(table.client.underlying.deleteItem(req))
      .map(_ => true)
      .optApp[ConditionExpression[Boolean]](f =>
        _ => f.recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      )(condition)
      .optApp[ConditionExpression[Boolean]](f =>
        _ => f.recover { case _: model.ConditionalCheckFailedException => false }
      )(condition)
  }

  override protected def doDeleteAndReturn[V, PK, O: DynamoDBOverride[Client, *]](
    table: Table[Aws2DynamoDBClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    overrides: O
  ): F[ConditionResult[V]] = {
    val resolvedOverride = (table.overrides |+| overrides)(table.client)
    val req = Aws2TableOps(table).delete(key, condition, returnOld = true, overrides = resolvedOverride).build()

    async(table.client.underlying.deleteItem(req))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws2TableOps(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override def createTable[O: DynamoDBOverride[Client, *]](
    client: Aws2DynamoDBClient,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)],
    overrides: O = DynamoDBOverride.Empty
  ): F[Unit] = {
    val resolvedOverride = overrides.overrides[Client].apply(client)
    val req = Aws2TableOps
      .createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes, resolvedOverride)
      .build()
    async(client.underlying.createTable(req)).map(_ => ())
  }

  override def dropTable[O: DynamoDBOverride[Client, *]](
    client: Aws2DynamoDBClient,
    tableName: String,
    overrides: O
  ): F[Unit] = {
    val resolvedOverride = overrides.overrides[Aws2DynamoDBClient].apply(client)
    val req = Aws2TableOps.dropTable(tableName, resolvedOverride).build()
    async(client.underlying.deleteTable(req)).map(_ => ())
  }

  override def batchGetRequest[V, PK](
    table: Table[Aws2DynamoDBClient, V, PK],
    key: PK
  ): java.util.Map[String, AttributeValue] =
    table.schema.serializePK(key)

  override def batchGet(
    client: Aws2DynamoDBClient,
    keys: Map[String, Seq[util.Map[String, AttributeValue]]]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue]] =
    batchGet(client, keys.view.mapValues({ kv => KeysAndAttributes.builder().keys(kv.asJava).build() }).toMap.asJava)

  override def batchGet(
    client: Aws2DynamoDBClient,
    keys: util.Map[String, KeysAndAttributes]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue]] =
    async(client.underlying.batchGetItem(model.BatchGetItemRequest.builder().requestItems(keys).build()))
      .map(Aws2BatchRead(_))

  override def batchPutRequest[V, PK](table: Table[Aws2DynamoDBClient, V, PK], value: V): WriteRequest =
    WriteRequest.builder().putRequest(Aws2TableOps(table).putRequest(value).build()).build()

  override def batchDeleteRequest[V, PK](table: Table[Aws2DynamoDBClient, V, PK], key: PK): WriteRequest =
    WriteRequest.builder().deleteRequest(Aws2TableOps(table).deleteRequest(key).build()).build()

  override def batchWrite(
    client: Aws2DynamoDBClient,
    values: util.Map[String, util.List[WriteRequest]]
  ): F[Aws2BatchWrite] =
    async(client.underlying.batchWriteItem(BatchWriteItemRequest.builder().requestItems(values).build()))
      .map(Aws2BatchWrite(_))
}
