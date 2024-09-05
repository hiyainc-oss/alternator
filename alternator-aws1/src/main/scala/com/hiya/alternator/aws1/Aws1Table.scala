package com.hiya.alternator.aws1

import cats.syntax.all._
import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator.internal.{ConditionalSupport, OptApp}
import com.hiya.alternator.schema.DynamoFormat.Result
import com.hiya.alternator.schema.{DynamoFormat, ScalarType}
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.{BatchReadResult, BatchWriteResult, TableLike}

import java.util
import scala.jdk.CollectionConverters._

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

  override def unprocessedItems[V, PK](table: TableLike[_, V, PK]): Vector[Either[Result[PK], Result[V]]] =
    unprocessedAvFor(table.tableName).map(_.bimap(table.schema.extract(_), Aws1Table(table).deserialize))
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

  override def processedItems[V, PK](table: TableLike[_, V, PK]): Vector[Result[V]] =
    processedAvFor(table.tableName).map(Aws1Table(table).deserialize)

  override def unprocessed: util.Map[String, KeysAndAttributes] =
    response.getUnprocessedKeys

  override def unprocessedAv: Map[String, Vector[util.Map[String, AttributeValue]]] =
    unprocessed.asScala.map { case (table, keys) => table -> keys.getKeys.asScala.toVector }.toMap

  override def unprocessedAvFor(table: String): Vector[util.Map[String, AttributeValue]] =
    unprocessed.getOrDefault(table, new KeysAndAttributes()).getKeys.asScala.toVector

  override def unprocessedKeys[V, PK](table: TableLike[_, V, PK]): Vector[Result[PK]] =
    unprocessedAvFor(table.tableName).map(table.schema.extract(_))
}

object Aws1BatchRead {
  def apply(response: BatchGetItemResult): Aws1BatchRead = new Aws1BatchRead(response)
}

class Aws1Table[V, PK](val underlying: TableLike[_, V, PK]) extends AnyVal {
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

  final def get(pk: PK, consistent: Boolean): GetItemRequest =
    new GetItemRequest(tableName, schema.serializePK(pk)).withConsistentRead(consistent)

  final def scan(
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]],
    limit: Option[Int],
    consistent: Boolean
  ): ScanRequest = {
    val request = new ScanRequest(tableName)
      .optApp(req =>
        (segment: Segment) => {
          req.withSegment(segment.segment).withTotalSegments(segment.totalSegments)
        }
      )(segment)
      .optApp(_.withLimit)(limit.map(Int.box))
      .withConsistentRead(consistent)

    condition match {
      case Some(condition) => ConditionalSupport.eval(request, condition)
      case None => request
    }
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

  final def put(item: V): PutItemRequest = put(item, returnOld = false)

  final def put(item: V, returnOld: Boolean): PutItemRequest = {
    val ret = new PutItemRequest(tableName, schema.serializeValue.writeFields(item))
    if (returnOld) ret.withReturnValues(ReturnValue.ALL_OLD) else ret.withReturnValues(ReturnValue.NONE)
  }

  final def put(item: V, condition: ConditionExpression[Boolean], returnOld: Boolean = false): PutItemRequest = {
    val ret = ConditionalSupport.eval(put(item), condition)
    if (returnOld) ret.withReturnValues(ReturnValue.ALL_OLD) else ret.withReturnValues(ReturnValue.NONE)
  }

  final def delete(key: PK): DeleteItemRequest = delete(key, returnOld = false)

  final def delete(key: PK, returnOld: Boolean): DeleteItemRequest = {
    val ret = new DeleteItemRequest(tableName, schema.serializePK(key))
    if (returnOld) ret.withReturnValues(ReturnValue.ALL_OLD) else ret.withReturnValues(ReturnValue.NONE)
  }

  final def delete(key: PK, condition: ConditionExpression[Boolean], returnOld: Boolean = false): DeleteItemRequest = {
    val ret = ConditionalSupport.eval(delete(key), condition)
    if (returnOld) ret.withReturnValues(ReturnValue.ALL_OLD) else ret.withReturnValues(ReturnValue.NONE)
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

  def putRequest(value: V): PutRequest = new PutRequest().withItem(schema.serializeValue.writeFields(value))

  def deleteRequest(key: PK): DeleteRequest = new DeleteRequest().withKey(schema.serializePK(key))

}

object Aws1Table {
  type AV = java.util.Map[String, AttributeValue]
  @inline def apply[V, PK](underlying: TableLike[_, V, PK]) = new Aws1Table(underlying)

  def dropTable(tableName: String): DeleteTableRequest =
    new DeleteTableRequest(tableName)

  def createTable(
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    readCapacity: Long,
    writeCapacity: Long,
    attributes: List[(String, ScalarType)]
  ): CreateTableRequest = {
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
