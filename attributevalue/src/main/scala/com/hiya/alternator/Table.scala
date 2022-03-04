package com.hiya.alternator

import com.hiya.alternator.Table.AV
import com.hiya.alternator.util._
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}


class Table[V, PK](val tableName: String, val schema: TableSchema.Aux[V, PK]) {
  sealed trait ItemMagnet[T] {
    def key(t: T): PK
  }

  object ItemMagnet {
    implicit object WholeItem extends ItemMagnet[V] {
      override def key(t: V): PK = schema.extract(t)
    }

    implicit object ItemKey extends ItemMagnet[PK] {
      override def key(t: PK): PK = t
    }
  }

  final def deserialize(response: AV): DynamoFormat.Result[V] = {
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
    ScanRequest.builder()
      .tableName(tableName)
      .optApp(req => (segment: Segment) => req.segment(segment.segment).totalSegments(segment.totalSegments))(segment)
  }

  final def batchGet(items: Seq[PK]): BatchGetItemRequest.Builder = {
    BatchGetItemRequest
      .builder()
      .requestItems(Map(tableName ->
        KeysAndAttributes.builder().keys(
          items.map(item => schema.serializePK(item)).asJava
        ).build()
      ).asJava)
  }

  final def put(item: V): PutItemRequest.Builder =
    PutItemRequest.builder().item(schema.serializeValue.writeFields(item)).tableName(tableName)

  final def batchPut(items: Seq[V]): BatchWriteItemRequest.Builder = {
    batchWrite(items.map(x => Right(x)))
  }

  final def delete(key: PK): DeleteItemRequest.Builder =
    DeleteItemRequest.builder().key(schema.serializePK(key)).tableName(tableName)

  final def batchDelete[T](items: Seq[T])(implicit T : ItemMagnet[T]): BatchWriteItemRequest.Builder =
    batchWrite(items.map(x => Left(T.key(x))))

  final def batchWrite(items: Seq[Either[PK, V]]): BatchWriteItemRequest.Builder = {
    BatchWriteItemRequest
      .builder()
      .requestItems(Map(tableName -> items.map {
        case Left(pk) =>
          WriteRequest.builder().deleteRequest(
            DeleteRequest.builder().key(schema.serializePK(pk)).build()
          ).build()
        case Right(item) =>
          WriteRequest.builder().putRequest(
            PutRequest.builder().item(schema.serializeValue.writeFields(item)).build()
          ).build()
      }.asJava).asJava)

  }

  def withClient(client: Client): client.PKClient[V, PK] = client.createPkClient(this)
}

object Table {
  type AV = java.util.Map[String, AttributeValue]
  type PK = (String, AV)

  final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

  final def orFail[T](x: DynamoFormat.Result[T]): Try[T] = x match {
    case Left(error) => Failure(DynamoDBException(error))
    case Right(value) => Success(value)
  }

  def tableWithPK[V](name: String)(
    implicit tableSchema: TableSchema[V]
  ): Table[V, tableSchema.IndexType] = new Table[V, tableSchema.IndexType](name, tableSchema)

  def tableWithRK[V](name: String)(
    implicit tableSchema: TableSchemaWithRange[V]
  ): TableWithRangeKey[V, tableSchema.PK, tableSchema.RK] =
    new TableWithRangeKey[V, tableSchema.PK, tableSchema.RK](name, tableSchema)

}
