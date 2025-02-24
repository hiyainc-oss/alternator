package com.hiya.alternator

import com.hiya.alternator.schema._

import scala.util.{Failure, Success, Try}

object Client {
  sealed trait Missing
  object Missing extends Missing
}

sealed class Table[C, V, PK](
  val client: C,
  val tableName: String,
  val schema: TableSchema.Aux[V, PK]
) {
  def withClient[C1](client: C1): Table[C1, V, PK] =
    new Table[C1, V, PK](client, tableName, schema)

  def noClient: Table[Client.Missing, V, PK] =
    new Table[Client.Missing, V, PK](Client.Missing, tableName, schema)
}

class TableWithRange[C, V, PK, RK](c: C, name: String, override val schema: TableSchemaWithRange.Aux[V, PK, RK])
  extends Table[C, V, (PK, RK)](c, name, schema) {
  override def withClient[C1](client: C1): TableWithRange[C1, V, PK, RK] =
    new TableWithRange[C1, V, PK, RK](client, name, schema)

  override def noClient: TableWithRange[Client.Missing, V, PK, RK] =
    new TableWithRange[Client.Missing, V, PK, RK](Client.Missing, tableName, schema)
}

final case class DynamoDBException(error: DynamoAttributeError) extends Exception(error.message)

object Table {
  final def orFail[T](x: DynamoFormat.Result[T]): Try[T] = x match {
    case Left(error) => Failure(DynamoDBException(error))
    case Right(value) => Success(value)
  }

  def tableWithPK[V](name: String)(implicit
    tableSchema: TableSchema[V]
  ): Table[Client.Missing, V, tableSchema.IndexType] =
    new Table[Client.Missing, V, tableSchema.IndexType](Client.Missing, name, tableSchema)

  def tableWithRK[V](name: String)(implicit
    tableSchema: TableSchemaWithRange[V]
  ): TableWithRange[Client.Missing, V, tableSchema.PK, tableSchema.RK] =
    new TableWithRange[Client.Missing, V, tableSchema.PK, tableSchema.RK](Client.Missing, name, tableSchema)
}
