package com.hiya.alternator

import com.hiya.alternator.schema._

import scala.util.{Failure, Success, Try}

sealed trait TableLike[C <: DynamoDBClient, V, PK] {
  val client: C
  val tableName: String
  val schema: TableSchema.Aux[V, PK]
  val overrides: DynamoDBOverride[C] = DynamoDBOverride.empty[C]
  val indexNameOpt: Option[String] = None

  def withClient[C1 <: DynamoDBClient](
    client: C1,
    overrides: DynamoDBOverride[C1] = DynamoDBOverride.empty[C1]
  ): TableLike[C1, V, PK] = {
    val parent = this
    val c = client
    val o = overrides
    new TableLike[C1, V, PK] {
      override val client: C1 = c
      override val tableName: String = parent.tableName
      override val schema: TableSchema.Aux[V, PK] = parent.schema
      override val overrides: DynamoDBOverride[C1] = o
    }
  }

  def withOverrides(overrides: DynamoDBOverride[C]): TableLike[C, V, PK] =
    withClient(this.client, overrides)

  def noClient: TableLike[DynamoDBClient.Missing, V, PK] =
    withClient(DynamoDBClient.Missing)
}

sealed trait TableWithRangeLike[C <: DynamoDBClient, V, PK, RK] extends TableLike[C, V, (PK, RK)] {
  override val schema: TableSchemaWithRange.Aux[V, PK, RK]

  override def withClient[C1 <: DynamoDBClient](
    client: C1,
    overrides: DynamoDBOverride[C1] = DynamoDBOverride.empty[C1]
  ): TableWithRangeLike[C1, V, PK, RK] = {
    val parent = this
    val c = client
    val o = overrides
    new TableWithRangeLike[C1, V, PK, RK] {
      override val client: C1 = c
      override val tableName: String = parent.tableName
      override val schema: TableSchemaWithRange.Aux[V, PK, RK] = parent.schema
      override val overrides: DynamoDBOverride[C1] = o
    }
  }

  override def withOverrides(overrides: DynamoDBOverride[C]): TableWithRangeLike[C, V, PK, RK] =
    withClient(this.client, overrides)

  override def noClient: TableWithRangeLike[DynamoDBClient.Missing, V, PK, RK] =
    withClient(DynamoDBClient.Missing)
}

sealed class Table[C <: DynamoDBClient, V, PK](
  override val client: C,
  override val tableName: String,
  override val schema: TableSchema.Aux[V, PK],
  override val overrides: DynamoDBOverride[C] = DynamoDBOverride.empty[C]
) extends TableLike[C, V, PK] {
  override def withClient[C1 <: DynamoDBClient](
    client: C1,
    overrides: DynamoDBOverride[C1] = DynamoDBOverride.empty[C1]
  ): Table[C1, V, PK] =
    new Table[C1, V, PK](client, tableName, schema, overrides)

  override def withOverrides(overrides: DynamoDBOverride[C]): Table[C, V, PK] =
    new Table[C, V, PK](client, tableName, schema, overrides)

  override def noClient: Table[DynamoDBClient.Missing, V, PK] =
    new Table[DynamoDBClient.Missing, V, PK](DynamoDBClient.Missing, tableName, schema)

  def index[IPK](indexSchema: IndexSchema[V, IPK]): Index[C, V, IPK] =
    new Index(client, tableName, indexSchema.indexName, indexSchema.schema, overrides)

  def index[IPK, IRK](indexSchema: IndexSchemaWithRange[V, IPK, IRK]): IndexWithRange[C, V, IPK, IRK] =
    new IndexWithRange(client, tableName, indexSchema.indexName, indexSchema.schema, overrides)
}

class TableWithRange[C <: DynamoDBClient, V, PK, RK](
  c: C,
  name: String,
  override val schema: TableSchemaWithRange.Aux[V, PK, RK],
  overrides: DynamoDBOverride[C] = DynamoDBOverride.empty[C]
) extends Table[C, V, (PK, RK)](c, name, schema, overrides)
  with TableWithRangeLike[C, V, PK, RK] {

  override def withClient[C1 <: DynamoDBClient](
    client: C1,
    overrides: DynamoDBOverride[C1] = DynamoDBOverride.empty[C1]
  ): TableWithRange[C1, V, PK, RK] =
    new TableWithRange[C1, V, PK, RK](client, name, schema, overrides)

  override def withOverrides(overrides: DynamoDBOverride[C]): TableWithRange[C, V, PK, RK] =
    new TableWithRange[C, V, PK, RK](client, name, schema, overrides)

  override def noClient: TableWithRange[DynamoDBClient.Missing, V, PK, RK] =
    new TableWithRange[DynamoDBClient.Missing, V, PK, RK](DynamoDBClient.Missing, tableName, schema)
}

class Index[C <: DynamoDBClient, V, PK](
  override val client: C,
  override val tableName: String,
  val indexName: String,
  override val schema: TableSchemaWithPartition.Aux[V, PK],
  override val overrides: DynamoDBOverride[C] = DynamoDBOverride.empty[C]
) extends TableLike[C, V, PK] {
  override val indexNameOpt: Option[String] = Some(indexName)

  override def withClient[C1 <: DynamoDBClient](
    client: C1,
    overrides: DynamoDBOverride[C1] = DynamoDBOverride.empty[C1]
  ): Index[C1, V, PK] =
    new Index[C1, V, PK](client, tableName, indexName, schema, overrides)

  override def withOverrides(overrides: DynamoDBOverride[C]): Index[C, V, PK] =
    new Index[C, V, PK](client, tableName, indexName, schema, overrides)

  override def noClient: Index[DynamoDBClient.Missing, V, PK] =
    new Index[DynamoDBClient.Missing, V, PK](DynamoDBClient.Missing, tableName, indexName, schema)
}

class IndexWithRange[C <: DynamoDBClient, V, PK, RK](
  override val client: C,
  override val tableName: String,
  val indexName: String,
  override val schema: TableSchemaWithRange.Aux[V, PK, RK],
  override val overrides: DynamoDBOverride[C] = DynamoDBOverride.empty[C]
) extends TableWithRangeLike[C, V, PK, RK] {
  override val indexNameOpt: Option[String] = Some(indexName)

  override def withClient[C1 <: DynamoDBClient](
    client: C1,
    overrides: DynamoDBOverride[C1] = DynamoDBOverride.empty[C1]
  ): IndexWithRange[C1, V, PK, RK] =
    new IndexWithRange[C1, V, PK, RK](client, tableName, indexName, schema, overrides)

  override def withOverrides(overrides: DynamoDBOverride[C]): IndexWithRange[C, V, PK, RK] =
    new IndexWithRange[C, V, PK, RK](client, tableName, indexName, schema, overrides)

  override def noClient: IndexWithRange[DynamoDBClient.Missing, V, PK, RK] =
    new IndexWithRange[DynamoDBClient.Missing, V, PK, RK](DynamoDBClient.Missing, tableName, indexName, schema)
}

final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

object Table {
  final def orFail[T](x: DynamoFormat.Result[T]): Try[T] = x match {
    case Left(error) => Failure(DynamoDBException(error))
    case Right(value) => Success(value)
  }

  def tableWithPK[V](name: String)(implicit
    tableSchema: TableSchema[V]
  ): Table[DynamoDBClient.Missing, V, tableSchema.IndexType] =
    new Table[DynamoDBClient.Missing, V, tableSchema.IndexType](DynamoDBClient.Missing, name, tableSchema)

  def tableWithRK[V](name: String)(implicit
    tableSchema: TableSchemaWithRange[V]
  ): TableWithRange[DynamoDBClient.Missing, V, tableSchema.PK, tableSchema.RK] =
    new TableWithRange[DynamoDBClient.Missing, V, tableSchema.PK, tableSchema.RK](
      DynamoDBClient.Missing,
      name,
      tableSchema
    )
}
