package com.hiya.alternator

import com.hiya.alternator.schema._
import com.hiya.alternator.DynamoDBOverride._

import scala.util.{Failure, Success, Try}

sealed class Table[C <: DynamoDBClient, V, PK](
  val client: C,
  val tableName: String,
  val schema: TableSchema.Aux[V, PK],
  val overrides: DynamoDBOverride.Applicator[C] = DynamoDBOverride.Empty.overrides[C]
) {
  def withClient[C1 <: DynamoDBClient, O: DynamoDBOverride[C1, *]](
    client: C1,
    overrides: O = DynamoDBOverride.Empty
  ): Table[C1, V, PK] =
    new Table[C1, V, PK](client, tableName, schema, overrides)

  def withOverrides[O: DynamoDBOverride[C, *]](overrides: O): Table[C, V, PK] =
    new Table[C, V, PK](client, tableName, schema, overrides.overrides[C])

  def noClient: Table[DynamoDBClient.Missing, V, PK] =
    new Table[DynamoDBClient.Missing, V, PK](DynamoDBClient.Missing, tableName, schema)
}

class TableWithRange[C <: DynamoDBClient, V, PK, RK](
  c: C,
  name: String,
  override val schema: TableSchemaWithRange.Aux[V, PK, RK],
  overrides: DynamoDBOverride.Applicator[C] = DynamoDBOverride.Empty.overrides[C]
) extends Table[C, V, (PK, RK)](c, name, schema, overrides) {
  override def withClient[C1 <: DynamoDBClient, O: DynamoDBOverride[C1, *]](
    client: C1,
    overrides: O = DynamoDBOverride.Empty
  ): TableWithRange[C1, V, PK, RK] =
    new TableWithRange[C1, V, PK, RK](client, name, schema, overrides)

  override def withOverrides[O: DynamoDBOverride[C, *]](overrides: O): TableWithRange[C, V, PK, RK] =
    new TableWithRange[C, V, PK, RK](client, name, schema, overrides.overrides[C])

  override def noClient: TableWithRange[DynamoDBClient.Missing, V, PK, RK] =
    new TableWithRange[DynamoDBClient.Missing, V, PK, RK](DynamoDBClient.Missing, tableName, schema)
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
