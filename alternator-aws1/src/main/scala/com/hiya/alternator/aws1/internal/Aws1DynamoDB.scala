package com.hiya.alternator.aws1.internal

import cats.MonadThrow
import cats.syntax.all._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, model}
import com.hiya.alternator._
import com.hiya.alternator.aws1._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.ScalarType
import com.hiya.alternator.syntax.ConditionExpression

import java.util
import java.util.concurrent.{CompletionException, Future => JFuture}
import scala.jdk.CollectionConverters._
import scala.collection.compat._

abstract class Aws1DynamoDB[F[_]: MonadThrow, S[_]] extends DynamoDB[F] {
  override type Source[T] = S[T]
  override type Client = AmazonDynamoDBAsync

  override type AttributeValue = model.AttributeValue
  override type BatchReadItemRequest = KeysAndAttributes
  override type BatchReadItemResponse = model.BatchGetItemResult
  override type BatchWriteItemRequest = model.WriteRequest
  override type BatchWriteItemResponse = model.BatchWriteItemResult

  override def isRetryable(e: Throwable): Boolean = Exceptions.isRetryable(e)
  override def isThrottling(e: Throwable): Boolean = Exceptions.isThrottle(e)

  protected def async[Req <: AmazonWebServiceRequest, Resp](f: AsyncHandler[Req, Resp] => JFuture[Resp]): F[Resp]

  override def AV: schema.AttributeValue[AttributeValue] = Aws1IsAttributeValues

  override def doGet[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    pk: PK,
    consistent: Boolean
  ): F[Option[Result[V]]] =
    async(
      table.client.getItemAsync(Aws1TableOps(table).get(pk, consistent), _: AsyncHandler[GetItemRequest, GetItemResult])
    )
      .map(Aws1TableOps(table).deserialize)

  override def doPut[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(
          table.client.putItemAsync(
            Aws1TableOps(table).put(item, condition),
            _: AsyncHandler[PutItemRequest, PutItemResult]
          )
        )
          .map(_ => true)
          .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        async(
          table.client
            .putItemAsync(
              Aws1TableOps(table).put(item, returnOld = false),
              _: AsyncHandler[PutItemRequest, PutItemResult]
            )
        )
          .map(_ => true)
    }

  override def doPutAndReturn[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    item: V,
    condition: Option[ConditionExpression[Boolean]]
  ): F[ConditionResult[V]] = {
    val req = condition match {
      case Some(condition) =>
        Aws1TableOps(table).put(item, condition, returnOld = true)
      case None =>
        Aws1TableOps(table).put(item, returnOld = true)
    }

    async(table.client.putItemAsync(req, _: AsyncHandler[PutItemRequest, PutItemResult]))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws1TableOps(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override def doDelete[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ): F[Boolean] =
    condition match {
      case Some(condition) =>
        async(
          table.client.deleteItemAsync(
            Aws1TableOps(table).delete(key, condition),
            _: AsyncHandler[DeleteItemRequest, DeleteItemResult]
          )
        )
          .map(_ => true)
          .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
          .recover { case _: model.ConditionalCheckFailedException => false }
      case None =>
        async(
          table.client
            .deleteItemAsync(Aws1TableOps(table).delete(key), _: AsyncHandler[DeleteItemRequest, DeleteItemResult])
        )
          .map(_ => true)
    }

  override def doDeleteAndReturn[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    key: PK,
    condition: Option[ConditionExpression[Boolean]]
  ): F[ConditionResult[V]] = {
    val req = condition match {
      case Some(condition) =>
        Aws1TableOps(table).delete(key, condition, returnOld = true)
      case None =>
        Aws1TableOps(table).delete(key, returnOld = true)
    }

    async(table.client.deleteItemAsync(req, _: AsyncHandler[DeleteItemRequest, DeleteItemResult]))
      .map[ConditionResult[V]](item => ConditionResult.Success(Aws1TableOps(table).extractItem(item)))
      .recoverWith { case ex: CompletionException => MonadThrow[F].raiseError(ex.getCause) }
      .recover { case _: model.ConditionalCheckFailedException => ConditionResult.Failed }
  }

  override def createTable(
    client: AmazonDynamoDBAsync,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)]
  ): F[Unit] =
    async(
      client.createTableAsync(
        Aws1TableOps.createTable(tableName, hashKey, rangeKey, readCapacity, writeCapacity, attributes),
        _: AsyncHandler[CreateTableRequest, CreateTableResult]
      )
    ).map(_ => ())

  override def dropTable(client: AmazonDynamoDBAsync, tableName: String): F[Unit] = {
    async(
      client.deleteTableAsync(
        Aws1TableOps.dropTable(tableName),
        _: AsyncHandler[DeleteTableRequest, DeleteTableResult]
      )
    ).map(_ => ())
  }

  override def batchGetRequest[V, PK](
    table: Table[AmazonDynamoDBAsync, V, PK],
    key: PK
  ): java.util.Map[String, AttributeValue] =
    table.schema.serializePK(key)

  override def batchGet(
    client: AmazonDynamoDBAsync,
    keys: Map[String, Seq[util.Map[String, AttributeValue]]]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResult, AttributeValue]] = {
    batchGet(client, keys.view.mapValues(v => new KeysAndAttributes().withKeys(v.asJava)).toMap.asJava)
  }

  override def batchGet(
    client: AmazonDynamoDBAsync,
    keys: util.Map[String, KeysAndAttributes]
  ): F[BatchReadResult[KeysAndAttributes, BatchGetItemResult, AttributeValue]] =
    async(
      client.batchGetItemAsync(
        new model.BatchGetItemRequest(keys),
        _: AsyncHandler[model.BatchGetItemRequest, model.BatchGetItemResult]
      )
    ).map(Aws1BatchRead(_))

  override def batchPutRequest[V, PK](table: Table[AmazonDynamoDBAsync, V, PK], value: V): model.WriteRequest =
    new model.WriteRequest().withPutRequest(Aws1TableOps(table).putRequest(value))

  override def batchDeleteRequest[V, PK](table: Table[AmazonDynamoDBAsync, V, PK], key: PK): model.WriteRequest =
    new model.WriteRequest().withDeleteRequest(Aws1TableOps(table).deleteRequest(key))

  override def batchWrite(
    client: AmazonDynamoDBAsync,
    values: util.Map[String, util.List[WriteRequest]]
  ): F[BatchWriteResult[WriteRequest, BatchWriteItemResult, AttributeValue]] =
    async(
      client.batchWriteItemAsync(
        new model.BatchWriteItemRequest(values),
        _: AsyncHandler[model.BatchWriteItemRequest, BatchWriteItemResult]
      )
    ).map(Aws1BatchWrite(_))
}
