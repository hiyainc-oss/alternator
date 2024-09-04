package com.hiya.alternator.aws2

import cats.syntax.all._
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.{BatchReadResult, BatchWriteResult, TableLike}
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

  override def unprocessedItems[V, PK](table: TableLike[_, V, PK]): Vector[Either[Result[PK], Result[V]]] =
    unprocessedAvFor(table.tableName).map(_.bimap(table.schema.extract(_), Aws2Table(table).deserialize))
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

  override def processedItems[V, PK](table: TableLike[_, V, PK]): Vector[Result[V]] =
    processedAvFor(table.tableName).map(Aws2Table(table).deserialize)

  override def unprocessed: util.Map[String, KeysAndAttributes] =
    response.unprocessedKeys()

  override def unprocessedAv: Map[String, Vector[util.Map[String, AttributeValue]]] =
    unprocessed.asScala.map { case (table, keys) => table -> keys.keys().asScala.toVector }.toMap

  override def unprocessedAvFor(table: String): Vector[util.Map[String, AttributeValue]] =
    unprocessed.getOrDefault(table, KeysAndAttributes.builder().build()).keys().asScala.toVector

  override def unprocessedKeys[V, PK](table: TableLike[_, V, PK]): Vector[Result[PK]] =
    unprocessedAvFor(table.tableName).map(table.schema.extract(_))
}

object Aws2BatchRead {
  def apply(response: BatchGetItemResponse): Aws2BatchRead = new Aws2BatchRead(response)
}

class Aws2Table[V, PK](val underlying: TableLike[_, V, PK]) extends AnyVal {
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

  final def put(item: V): PutItemRequest.Builder = put(item, returnOld = false)

  final def put(item: V, returnOld: Boolean): PutItemRequest.Builder = {
    val ret = PutItemRequest.builder().item(schema.serializeValue.writeFields(item)).tableName(tableName)
    if (returnOld) ret.returnValues(ReturnValue.ALL_OLD) else ret.returnValues(ReturnValue.NONE)
  }

  final def put(
    item: V,
    condition: ConditionExpression[Boolean],
    returnOld: Boolean = false
  ): PutItemRequest.Builder = {
    val ret = ConditionalSupport(put(item), condition)
    if (returnOld) ret.returnValues(ReturnValue.ALL_OLD) else ret.returnValues(ReturnValue.NONE)
  }

  final def delete(key: PK): DeleteItemRequest.Builder =
    delete(key, returnOld = false)

  final def delete(key: PK, returnOld: Boolean): DeleteItemRequest.Builder = {
    val ret = DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(tableName)
    if (returnOld) ret.returnValues(ReturnValue.ALL_OLD) else ret.returnValues(ReturnValue.NONE)
  }

  final def delete(
    key: PK,
    condition: ConditionExpression[Boolean],
    returnOld: Boolean = false
  ): DeleteItemRequest.Builder = {
    val ret = ConditionalSupport(delete(key), condition)
    if (returnOld) ret.returnValues(ReturnValue.ALL_OLD) else ret.returnValues(ReturnValue.NONE)
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

object Aws2Table {
  type AV = java.util.Map[String, AttributeValue]

  @inline def apply[V, PK](underlying: TableLike[_, V, PK]) = new Aws2Table(underlying)

  def dropTable(tableName: String): DeleteTableRequest.Builder =
    DeleteTableRequest.builder().tableName(tableName)

  def createTable(
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)]
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
  }
}
