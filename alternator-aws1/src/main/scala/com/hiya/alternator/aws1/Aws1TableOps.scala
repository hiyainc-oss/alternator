package com.hiya.alternator.aws1

import cats.syntax.all._
import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator.internal.{ConditionalSupport, OptApp}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.{BatchReadResult, BatchWriteResult, Table}

import java.util
import scala.jdk.CollectionConverters._
import com.hiya.alternator.aws1.Aws1DynamoDBClient
import com.hiya.alternator.DynamoDBOverride

class Aws1BatchWrite(override val response: BatchWriteItemResult)
  extends AnyVal
  with BatchWriteResult[WriteRequest, BatchWriteItemResult, AttributeValue] {

  override def AV: com.hiya.alternator.schema.AttributeValue[AttributeValue] = Aws1IsAttributeValues

  override def unprocessed: util.Map[String, util.List[WriteRequest]] =
    response.getUnprocessedItems

  override def unprocessedAv
    : Map[String, Vector[Either[util.Map[String, AttributeValue], util.Map[String, AttributeValue]]]] =
    unprocessed.asScala.map { case (table, items) => table -> getAVs(items) }.toMap

  private def getAVs(
    unprocessed: java.util.List[WriteRequest]
  ): Vector[Either[util.Map[String, AttributeValue], util.Map[String, AttributeValue]]] =
    unprocessed.asScala.map { item =>
      (Option(item.getPutRequest), Option(item.getDeleteRequest)) match {
        case (None, None) => throw new IllegalStateException()
        case (None, Some(delete)) => Left(delete.getKey)
        case (Some(put), None) => Right(put.getItem)
        case _ => throw new IllegalStateException()
      }
    }.toVector

  override def unprocessedAvFor(
    table: String
  ): Vector[Either[util.Map[String, AttributeValue], util.Map[String, AttributeValue]]] =
    getAVs(unprocessed.getOrDefault(table, java.util.List.of()))

  override def unprocessedItems[V, PK](table: Table[_, V, PK]): Vector[Either[Result[PK], Result[V]]] =
    unprocessedAvFor(table.tableName).map(
      _.bimap(table.schema.extract(_), new com.hiya.alternator.aws1.Aws1TableOps(table).deserialize)
    )
}

object Aws1BatchWrite {
  def apply(response: BatchWriteItemResult): Aws1BatchWrite = new Aws1BatchWrite(response)
}

class Aws1BatchRead(
  override val response: BatchGetItemResult
) extends AnyVal
  with BatchReadResult[KeysAndAttributes, BatchGetItemResult, AttributeValue] {
  override def AV: com.hiya.alternator.schema.AttributeValue[AttributeValue] = Aws1IsAttributeValues

  override def processed: util.Map[String, util.List[util.Map[String, AttributeValue]]] = response.getResponses

  override def processedAv: Map[String, Vector[util.Map[String, AttributeValue]]] =
    processed.asScala.map { case (table, items) => table -> items.asScala.toVector }.toMap

  override def processedAvFor(table: String): Vector[util.Map[String, AttributeValue]] =
    processed.getOrDefault(table, List.empty.asJava).asScala.toVector

  override def processedItems[V, PK](table: Table[_, V, PK]): Vector[Result[V]] =
    processedAvFor(table.tableName).map(Aws1TableOps(table).deserialize)

  override def unprocessed: util.Map[String, KeysAndAttributes] =
    response.getUnprocessedKeys

  override def unprocessedAv: Map[String, Vector[util.Map[String, AttributeValue]]] =
    unprocessed.asScala.map { case (table, keys) => table -> keys.getKeys.asScala.toVector }.toMap

  override def unprocessedAvFor(table: String): Vector[util.Map[String, AttributeValue]] =
    unprocessed.getOrDefault(table, new KeysAndAttributes()).getKeys.asScala.toVector

  override def unprocessedKeys[V, PK](table: Table[_, V, PK]): Vector[Result[PK]] =
    unprocessedAvFor(table.tableName).map(table.schema.extract(_))
}

object Aws1BatchRead {
  def apply(response: BatchGetItemResult): Aws1BatchRead = new Aws1BatchRead(response)
}

final class Aws1TableOps[V, PK](val underlying: com.hiya.alternator.Table[_, V, PK]) {
  final def deserialize(response: Aws1TableOps.AV): DynamoFormat.Result[V] = {
    underlying.schema.serializeValue.readFields(response)
  }

  final def deserialize(response: GetItemResult): Option[DynamoFormat.Result[V]] = {
    Option(response.getItem).map(deserialize)
  }

  final def deserialize(response: ScanResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(deserialize))
  }

  final def get(
    pk: PK,
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): GetItemRequest =
    overrides(
      new GetItemRequest(underlying.tableName, underlying.schema.serializePK(pk)).withConsistentRead(consistent)
    )

  final def scan(
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]],
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): ScanRequest = {
    val request = new ScanRequest(underlying.tableName)
      .optApp(req =>
        (segment: Segment) => {
          req.withSegment(segment.segment).withTotalSegments(segment.totalSegments)
        }
      )(segment)
      .withConsistentRead(consistent)
      .optApp[ConditionExpression[Boolean]](req => cond => ConditionalSupport.eval(req, cond))(condition)

    overrides(request)
  }

  final def batchGet(
    items: Seq[PK],
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): BatchGetItemRequest = {
    val req = new BatchGetItemRequest(
      Map(
        underlying.tableName -> {
          new KeysAndAttributes().withKeys(items.map(item => underlying.schema.serializePK(item)).asJava)
        }
      ).asJava
    )

    overrides(req)
  }

  final def put(
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    returnOld: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): PutItemRequest = {
    val req = new PutItemRequest(underlying.tableName, underlying.schema.serializeValue.writeFields(item))
      .withReturnValues(if (returnOld) ReturnValue.ALL_OLD else ReturnValue.NONE)
      .optApp[ConditionExpression[Boolean]](req => cond => ConditionalSupport.eval(req, cond))(condition)

    overrides(req)
  }

  final def delete(
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    returnOld: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): DeleteItemRequest = {
    val req = new DeleteItemRequest(underlying.tableName, underlying.schema.serializePK(key))
      .withReturnValues(if (returnOld) ReturnValue.ALL_OLD else ReturnValue.NONE)
      .optApp[ConditionExpression[Boolean]](req => cond => ConditionalSupport.eval(req, cond))(condition)

    overrides(req)
  }

  final def extractItem(item: DeleteItemResult): Option[Result[V]] = {
    Option(item.getAttributes)
      .filterNot(_.isEmpty)
      .map(deserialize)
  }

  final def extractItem(item: PutItemResult): Option[Result[V]] = {
    Option(item.getAttributes)
      .filterNot(_.isEmpty)
      .map(deserialize)
  }

  def putRequest(value: V): PutRequest = new PutRequest().withItem(underlying.schema.serializeValue.writeFields(value))

  def deleteRequest(key: PK): DeleteRequest = new DeleteRequest().withKey(underlying.schema.serializePK(key))

}

object Aws1TableOps {
  type AV = java.util.Map[String, AttributeValue]

  @inline def apply[V, PK](underlying: Table[_, V, PK]): Aws1TableOps[V, PK] =
    new Aws1TableOps[V, PK](underlying)

  def dropTable(
    tableName: String,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): DeleteTableRequest =
    overrides(new DeleteTableRequest(tableName))

  def createTable(
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)],
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): CreateTableRequest = {
    val keySchema: List[KeySchemaElement] = {
      new KeySchemaElement(hashKey, KeyType.HASH) ::
        rangeKey.map(key => new KeySchemaElement(key, KeyType.RANGE)).toList
    }

    val req = new CreateTableRequest(
      attributes.map { case (name, scalarType) =>
        new AttributeDefinition(name, scalarType)
      }.asJava,
      tableName,
      keySchema.asJava,
      new ProvisionedThroughput(readCapacity, writeCapacity)
    )

    overrides(req)
  }
}
