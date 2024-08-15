package com.hiya.alternator.aws2

import com.hiya.alternator.TableLike
import com.hiya.alternator.schema.{DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.util._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

class Aws2Table[V, PK](val underlying: TableLike[DynamoDbAsyncClient, V, PK]) extends AnyVal {
  import underlying._

  final def deserialize(response: Aws2Table.AV): DynamoFormat.Result[V] = {
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

  final def get(pk: PK): GetItemRequest.Builder =
    GetItemRequest.builder().key(schema.serializePK(pk)).tableName(tableName)

  final def scan(segment: Option[Segment] = None): ScanRequest.Builder = {
    ScanRequest
      .builder()
      .tableName(tableName)
      .optApp(req => (segment: Segment) => req.segment(segment.segment).totalSegments(segment.totalSegments))(segment)
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

  final def put(item: V): PutItemRequest.Builder =
    PutItemRequest.builder().item(schema.serializeValue.writeFields(item)).tableName(tableName)

  final def put(item: V, condition: ConditionExpression[Boolean]): PutItemRequest.Builder = {
    val renderedCondition = RenderedConditional.render(condition)
    renderedCondition(put(item))
  }

  final def delete(key: PK): DeleteItemRequest.Builder =
    DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(tableName)

  final def delete(key: PK, condition: ConditionExpression[Boolean]): DeleteItemRequest.Builder = {
    val renderedCondition = RenderedConditional.render(condition)
    renderedCondition(delete(key))
  }

  final def batchWrite(items: Seq[Either[PK, V]]): BatchWriteItemRequest.Builder = {
    BatchWriteItemRequest
      .builder()
      .requestItems(Map(tableName -> items.map {
        case Left(pk) =>
          WriteRequest
            .builder()
            .deleteRequest(
              DeleteRequest.builder().key(schema.serializePK(pk)).build()
            )
            .build()
        case Right(item) =>
          WriteRequest
            .builder()
            .putRequest(
              PutRequest.builder().item(schema.serializeValue.writeFields(item)).build()
            )
            .build()
      }.asJava).asJava)
  }
}

object Aws2Table {
  type AV = java.util.Map[String, AttributeValue]

  @inline def apply[V, PK](underlying: TableLike[DynamoDbAsyncClient, V, PK]) = new Aws2Table(underlying)

  def dropTable(tableName: String): DeleteTableRequest.Builder =
    DeleteTableRequest.builder().tableName(tableName)

  def createTable(tableName: String, hashKey: String, rangeKey: Option[String], readCapacity: Long, writeCapacity: Long, attributes: List[(String, ScalarType)]): CreateTableRequest.Builder = {
    val keySchema: List[KeySchemaElement] = {
      KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build() ::
      rangeKey.map(key => KeySchemaElement.builder().attributeName(key).keyType(KeyType.RANGE).build()).toList
    }

    CreateTableRequest.builder()
      .tableName(tableName)
      .keySchema(keySchema.asJava)
      .attributeDefinitions(attributes.map { case (name, scalarType) =>
        AttributeDefinition.builder().attributeName(name).attributeType(scalarType).build()
      }.asJava)
      .provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(readCapacity).writeCapacityUnits(writeCapacity).build()
      )
  }
}
