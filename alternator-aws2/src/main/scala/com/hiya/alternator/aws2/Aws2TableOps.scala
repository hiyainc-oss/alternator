package com.hiya.alternator.aws2

import cats.syntax.all._
import com.hiya.alternator.aws2.{Aws2DynamoDBClient, Aws2Table}
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.{BatchReadResult, BatchWriteResult, DynamoDBOverride, Table}
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.model._

import java.util
import scala.jdk.CollectionConverters._

class Aws2BatchWrite(
  override val response: BatchWriteItemResponse
) extends AnyVal
  with BatchWriteResult[WriteRequest, BatchWriteItemResponse, AttributeValue] {
  override def AV: com.hiya.alternator.schema.AttributeValue[AttributeValue] = Aws2IsAttributeValues

  override def unprocessed: util.Map[String, util.List[WriteRequest]] =
    response.unprocessedItems()

  private def getAVs(
    unprocessed: java.util.List[WriteRequest]
  ): Vector[Either[util.Map[String, AttributeValue], util.Map[String, AttributeValue]]] =
    unprocessed.asScala.map { item =>
      (Option(item.putRequest()), Option(item.deleteRequest())) match {
        case (None, None) => throw new IllegalStateException()
        case (None, Some(delete)) => Left(delete.key())
        case (Some(put), None) => Right(put.item())
        case _ => throw new IllegalStateException()
      }
    }.toVector

  override def unprocessedAv
    : Map[String, Vector[Either[util.Map[String, AttributeValue], util.Map[String, AttributeValue]]]] =
    unprocessed.asScala.map { case (table, items) => table -> getAVs(items) }.toMap

  override def unprocessedAvFor(
    table: String
  ): Vector[Either[util.Map[String, AttributeValue], util.Map[String, AttributeValue]]] =
    getAVs(unprocessed.getOrDefault(table, java.util.List.of()))

  override def unprocessedItems[V, PK](table: Table[_, V, PK]): Vector[Either[Result[PK], Result[V]]] =
    unprocessedAvFor(table.tableName).map(_.bimap(table.schema.extract(_), table.schema.serializeValue.readFields(_)))
}

object Aws2BatchWrite {
  def apply(response: BatchWriteItemResponse): Aws2BatchWrite = new Aws2BatchWrite(response)
}

class Aws2BatchRead(
  override val response: BatchGetItemResponse
) extends AnyVal
  with BatchReadResult[KeysAndAttributes, BatchGetItemResponse, AttributeValue] {
  override def AV: com.hiya.alternator.schema.AttributeValue[AttributeValue] = Aws2IsAttributeValues

  override def processed: util.Map[String, util.List[util.Map[String, AttributeValue]]] = response.responses()

  override def processedAv: Map[String, Vector[util.Map[String, AttributeValue]]] =
    processed.asScala.map { case (table, items) => table -> items.asScala.toVector }.toMap

  override def processedAvFor(table: String): Vector[util.Map[String, AttributeValue]] =
    processed.getOrDefault(table, List.empty.asJava).asScala.toVector

  override def processedItems[V, PK](table: Table[_, V, PK]): Vector[Result[V]] =
    processedAvFor(table.tableName).map(table.schema.serializeValue.readFields(_))

  override def unprocessed: util.Map[String, KeysAndAttributes] =
    response.unprocessedKeys()

  override def unprocessedAv: Map[String, Vector[util.Map[String, AttributeValue]]] =
    unprocessed.asScala.map { case (table, keys) => table -> keys.keys().asScala.toVector }.toMap

  override def unprocessedAvFor(table: String): Vector[util.Map[String, AttributeValue]] =
    unprocessed.getOrDefault(table, KeysAndAttributes.builder().build()).keys().asScala.toVector

  override def unprocessedKeys[V, PK](table: Table[_, V, PK]): Vector[Result[PK]] =
    unprocessedAvFor(table.tableName).map(table.schema.extract(_))
}

object Aws2BatchRead {
  def apply(response: BatchGetItemResponse): Aws2BatchRead = new Aws2BatchRead(response)
}

class Aws2TableOps[V, PK](val underlying: Table[Aws2DynamoDBClient, V, PK]) extends AnyVal {
  import underlying._

  final def deserialize(response: Aws2TableOps.AV): DynamoFormat.Result[V] = {
    schema.serializeValue.readFields(response)
  }

  final def deserialize(response: GetItemResponse): Option[DynamoFormat.Result[V]] = {
    if (response.hasItem) Option(response.item()).map(deserialize)
    else None
  }

  final def deserialize(response: ScanResponse): List[DynamoFormat.Result[V]] = {
    if (response.hasItems) response.items().asScala.toList.map(deserialize)
    else Nil
  }

  final def get(
    pk: PK,
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): GetItemRequest.Builder =
    GetItemRequest
      .builder()
      .key(schema.serializePK(pk))
      .tableName(tableName)
      .consistentRead(consistent)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())

  final def scan(
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]],
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): ScanRequest.Builder = {
    val request = ScanRequest
      .builder()
      .tableName(tableName)
      .optApp(req => (segment: Segment) => req.segment(segment.segment).totalSegments(segment.totalSegments))(segment)
      .consistentRead(consistent)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())

    condition match {
      case Some(cond) => ConditionalSupport.eval(request, cond)
      case None => request
    }
  }

  final def batchGet(items: Seq[PK]): BatchGetItemRequest.Builder = {
    BatchGetItemRequest
      .builder()
      .requestItems(
        Map(
          tableName ->
            KeysAndAttributes
              .builder()
              .keys(
                items.map(item => schema.serializePK(item)).asJava
              )
              .build()
        ).asJava
      )
  }

  final def put(
    item: V,
    condition: Option[ConditionExpression[Boolean]],
    returnOld: Boolean,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): PutItemRequest.Builder = {
    PutItemRequest
      .builder()
      .item(schema.serializeValue.writeFields(item))
      .tableName(tableName)
      .optApp[ConditionExpression[Boolean]](req => cond => ConditionalSupport.eval(req, cond))(condition)
      .returnValues(if (returnOld) ReturnValue.ALL_OLD else ReturnValue.NONE)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
  }

  final def delete(key: PK): DeleteItemRequest.Builder =
    delete(key, returnOld = false)

  final def delete(key: PK, returnOld: Boolean): DeleteItemRequest.Builder = {
    val ret = DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(tableName)
    if (returnOld) ret.returnValues(ReturnValue.ALL_OLD) else ret.returnValues(ReturnValue.NONE)
  }

  final def delete(
    key: PK,
    condition: Option[ConditionExpression[Boolean]],
    returnOld: Boolean = false,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): DeleteItemRequest.Builder = {
    DeleteItemRequest
      .builder()
      .key(schema.serializePK(key))
      .tableName(tableName)
      .optApp[ConditionExpression[Boolean]](req => cond => ConditionalSupport.eval(req, cond))(condition)
      .returnValues(if (returnOld) ReturnValue.ALL_OLD else ReturnValue.NONE)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
  }

  final def putRequest(item: V): PutRequest.Builder = {
    PutRequest.builder().item(schema.serializeValue.writeFields(item))
  }

  final def deleteRequest(key: PK): DeleteRequest.Builder = {
    DeleteRequest.builder().key(schema.serializePK(key))
  }

  final def getRequest(key: PK): java.util.Map[String, AttributeValue] = {
    schema.serializePK(key)
  }

  def extractItem(item: DeleteItemResponse): Option[Result[V]] = {
    Option(item.attributes())
      .filterNot(_.isEmpty)
      .map(deserialize)
  }

  def extractItem(item: PutItemResponse): Option[Result[V]] = {
    Option(item.attributes())
      .filterNot(_.isEmpty)
      .map(deserialize)
  }
}

object Aws2TableOps {
  type AV = java.util.Map[String, AttributeValue]

  @inline def apply[V, PK](underlying: Aws2Table[V, PK]): Aws2TableOps[V, PK] = new Aws2TableOps(underlying)

  def dropTable(
    tableName: String,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): DeleteTableRequest.Builder =
    DeleteTableRequest
      .builder()
      .tableName(tableName)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())

  def createTable(
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)],
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): CreateTableRequest.Builder = {
    val keySchema: List[KeySchemaElement] = {
      KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build() ::
        rangeKey.map(key => KeySchemaElement.builder().attributeName(key).keyType(KeyType.RANGE).build()).toList
    }

    CreateTableRequest
      .builder()
      .tableName(tableName)
      .keySchema(keySchema.asJava)
      .attributeDefinitions(attributes.map { case (name, scalarType) =>
        AttributeDefinition.builder().attributeName(name).attributeType(scalarType).build()
      }.asJava)
      .provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(readCapacity).writeCapacityUnits(writeCapacity).build()
      )
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
  }
}
