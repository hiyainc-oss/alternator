package com.hiya.alternator.aws2.internal

import cats.MonadThrow
import cats.syntax.all._
import com.hiya.alternator._
import com.hiya.alternator.aws2._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.ConditionExpression
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

import java.util
import java.util.concurrent.CompletionException
import scala.jdk.CollectionConverters._

trait Aws2Overrides {
  def apply(req: AwsRequestOverrideConfiguration.Builder): AwsRequestOverrideConfiguration.Builder
}

abstract class Aws2DynamoDB[F[_]: MonadThrow, PS[_]] extends DynamoDB[F] {
  override type C = DynamoDbAsyncClient
  override type S[T] = PS[T]
  override type AttributeValue = model.AttributeValue
  override type BatchReadItemRequest = KeysAndAttributes
  override type BatchReadItemResponse = model.BatchGetItemResponse
  override type BatchWriteItemRequest = model.WriteRequest
  override type BatchWriteItemResponse = model.BatchWriteItemResponse
  override type Overrides = Aws2Overrides

  override def AV: schema.AttributeValue[AttributeValue] = Aws2IsAttributeValues

  protected def async[T](f: => java.util.concurrent.CompletableFuture[T]): F[T]

  override def isRetryable(e: Throwable): Boolean = Exceptions.isRetryable(e)
  override def isThrottling(e: Throwable): Boolean = Exceptions.isThrottle(e)

  @inline private def optApp[B](
    overrides: Option[Aws2Overrides],
    builder: B,
    f: (B, AwsRequestOverrideConfiguration) => B
  ): B =
    overrides match {
      case Some(o) => f(builder, o(AwsRequestOverrideConfiguration.builder()).build())
      case None => builder
    }

  override def put[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    overrides: Option[Aws2Overrides]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(
          table.client.putItem(
            optApp[PutItemRequest.Builder](
              overrides,
              Aws2Table(table).put(item, condition),
              _.overrideConfiguration(_)
            ).build()
          )
        )
          .map(_ => true)
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        async(
          table.client.putItem(
            optApp[PutItemRequest.Builder](
              overrides,
              Aws2Table(table).put(item, returnOld = false),
              _.overrideConfiguration(_)
            ).build()
          )
        )
          .map(_ => true)
    }

  override def putAndReturn[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    overrides: Option[Aws2Overrides]
  ): F[ConditionResult[V]] = {
    val req = condition match {
      case Some(condition) =>
        Aws2Table(table).put(item, condition, returnOld = true)
      case None =>
        Aws2Table(table).put(item, returnOld = true)
    }

    async(table.client.putItem(optApp[PutItemRequest.Builder](overrides, req, _.overrideConfiguration(_)).build()))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws2Table(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override def deleteAndReturn[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    overrides: Option[Aws2Overrides]
  ): F[ConditionResult[V]] = {
    val req = condition match {
      case Some(condition) =>
        Aws2Table(table).delete(key, condition, returnOld = true)
      case None =>
        Aws2Table(table).delete(key, returnOld = true)
    }

    async(
      table.client.deleteItem(optApp[DeleteItemRequest.Builder](overrides, req, _.overrideConfiguration(_)).build())
    )
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws2Table(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override def delete[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    overrides: Option[Aws2Overrides]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(
          table.client.deleteItem(
            optApp[DeleteItemRequest.Builder](
              overrides,
              Aws2Table(table).delete(key, condition),
              _.overrideConfiguration(_)
            ).build()
          )
        )
          .map(_ => true)
          .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        async(
          table.client.deleteItem(
            optApp[DeleteItemRequest.Builder](
              overrides,
              Aws2Table(table).delete(key),
              _.overrideConfiguration(_)
            ).build()
          )
        )
          .map(_ => true)
    }

  override def get[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    pk: PK,
    consistent: Boolean,
    overrides: Option[Aws2Overrides]
  ): F[Option[Result[V]]] = {
    async(
      table.client.getItem(
        optApp[GetItemRequest.Builder](
          overrides,
          Aws2Table(table).get(pk, consistent),
          _.overrideConfiguration(_)
        ).build()
      )
    )
      .map(Aws2Table(table).deserialize)
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
          Aws2Table.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes).build()
        )
    ).map(_ => ())

  override def dropTable(client: DynamoDbAsyncClient, tableName: String): F[Unit] =
    async(client.deleteTable(model.DeleteTableRequest.builder().tableName(tableName).build())).map(_ => ())

  override def batchGetRequest[V, PK](
    table: TableLike[DynamoDbAsyncClient, V, PK],
    key: PK
  ): java.util.Map[String, AttributeValue] =
    table.schema.serializePK(key)

  override def batchGetAV(
    client: DynamoDbAsyncClient,
    keys: Map[String, Seq[util.Map[String, AttributeValue]]],
    overrides: Option[Aws2Overrides]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue]] =
    batchGet(
      client,
      keys.view.mapValues({ kv => KeysAndAttributes.builder().keys(kv.asJava).build() }).toMap.asJava,
      overrides
    )

  override def batchGet(
    client: DynamoDbAsyncClient,
    keys: util.Map[String, KeysAndAttributes],
    overrides: Option[Aws2Overrides]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue]] =
    async(
      client.batchGetItem(
        optApp[BatchGetItemRequest.Builder](
          overrides,
          model.BatchGetItemRequest.builder().requestItems(keys),
          _.overrideConfiguration(_)
        ).build()
      )
    ).map(Aws2BatchRead(_))

  override def batchPutRequest[V, PK](table: TableLike[DynamoDbAsyncClient, V, PK], value: V): WriteRequest =
    WriteRequest.builder().putRequest(Aws2Table(table).putRequest(value).build()).build()

  override def batchDeleteRequest[V, PK](table: TableLike[DynamoDbAsyncClient, V, PK], key: PK): WriteRequest =
    WriteRequest.builder().deleteRequest(Aws2Table(table).deleteRequest(key).build()).build()

  override def batchWrite(
    client: DynamoDbAsyncClient,
    values: util.Map[String, util.List[WriteRequest]],
    overrides: Option[Aws2Overrides]
  ): F[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]] =
    async(
      client.batchWriteItem(
        optApp[BatchWriteItemRequest.Builder](
          overrides,
          BatchWriteItemRequest.builder().requestItems(values),
          _.overrideConfiguration(_)
        ).build()
      )
    ).map[BatchWriteResult[BatchWriteItemRequest, BatchWriteItemResponse, AttributeValue]](Aws2BatchWrite(_))
}
