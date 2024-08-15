package com.hiya.alternator.aws1

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator.TableLike
import com.hiya.alternator.schema.{DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.util.OptApp

import scala.jdk.CollectionConverters._

class Aws1Table[V, PK](val underlying: TableLike[AmazonDynamoDBAsync, V, PK]) extends AnyVal {
  import underlying._

  final def deserialize(response: Aws1Table.AV): DynamoFormat.Result[V] = {
    schema.serializeValue.readFields(response)
  }

  final def deserialize(response: GetItemResult): Option[DynamoFormat.Result[V]] = {
    Option(response.getItem).map(deserialize)
  }

  final def deserialize(response: ScanResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(deserialize))
  }

  final def get(pk: PK): GetItemRequest =
    new GetItemRequest(tableName, schema.serializePK(pk))

  final def scan(segment: Option[Segment] = None): ScanRequest = {
    new ScanRequest(tableName)
      .optApp(req => (segment: Segment) => {
        req.withSegment(segment.segment).withTotalSegments(segment.totalSegments)
      })(segment)
  }

  final def batchGet(items: Seq[PK]): BatchGetItemRequest = {
    new BatchGetItemRequest(
        Map(
          tableName -> {
            new KeysAndAttributes().withKeys(items.map(item => schema.serializePK(item)).asJava)
          }
        ).asJava
      )
  }

  final def put(item: V): PutItemRequest =
    new PutItemRequest(tableName, schema.serializeValue.writeFields(item))

  final def put(item: V, condition: ConditionExpression[Boolean]): PutItemRequest = {
    val renderedCondition = RenderedConditional.render(condition)
    renderedCondition(put(item))
  }

  final def delete(key: PK): DeleteItemRequest =
    new DeleteItemRequest(tableName, schema.serializePK(key))

  final def delete(key: PK, condition: ConditionExpression[Boolean]): DeleteItemRequest = {
    val renderedCondition = RenderedConditional.render(condition)
    renderedCondition(delete(key))
  }

  final def batchWrite(items: Seq[Either[PK, V]]): BatchWriteItemRequest = {
    new BatchWriteItemRequest(
      Map(tableName -> items.map {
        case Left(pk) =>
          new WriteRequest()
            .withDeleteRequest(
              new DeleteRequest(schema.serializePK(pk))
            )
        case Right(item) =>
          new WriteRequest()
            .withPutRequest(
              new PutRequest(schema.serializeValue.writeFields(item))
            )
      }.asJava).asJava)
  }
}

object Aws1Table {
  type AV = java.util.Map[String, AttributeValue]
  @inline def apply[V, PK](underlying: TableLike[AmazonDynamoDBAsync, V, PK]) = new Aws1Table(underlying)

  def dropTable(tableName: String): DeleteTableRequest =
    new DeleteTableRequest(tableName)

  def createTable(tableName: String, hashKey: String, rangeKey: Option[String], readCapacity: Long, writeCapacity: Long, attributes: List[(String, ScalarType)]): CreateTableRequest = {
    val keySchema: List[KeySchemaElement] = {
      new KeySchemaElement(hashKey, KeyType.HASH) ::
      rangeKey.map(key => new KeySchemaElement(key, KeyType.RANGE)).toList
    }

    new CreateTableRequest(
      attributes.map { case (name, scalarType) =>
        new AttributeDefinition(name, scalarType)
      }.asJava,
      tableName,
      keySchema.asJava,
      new ProvisionedThroughput(readCapacity, writeCapacity)
    )
  }
}
