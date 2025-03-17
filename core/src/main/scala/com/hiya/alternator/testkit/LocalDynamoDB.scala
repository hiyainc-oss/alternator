package com.hiya.alternator.testkit

import com.hiya.alternator.schema._
import com.hiya.alternator.{DynamoDBClient, Table, TableWithRange}

import java.util.UUID

trait LocalDynamoClient[C <: DynamoDBClient] {
  type Config

  def config(port: Int): Config
  def client(port: Int): C
}

object LocalDynamoClient {
  type Aux[C <: DynamoDBClient, B] = LocalDynamoClient[C] { type Config = B }
}

object LocalDynamoDB {
  val DEFAULT_PORT = 8000
  val configuredPort: Int =
    Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(DEFAULT_PORT)

  def config[C <: DynamoDBClient](port: Int = configuredPort)(implicit
    localDynamoClient: LocalDynamoClient[C]
  ): localDynamoClient.Config =
    localDynamoClient.config(port)

  def client[C <: DynamoDBClient](port: Int = configuredPort)(implicit localDynamoClient: LocalDynamoClient[C]): C =
    localDynamoClient.client(port)

  def withTable[C <: DynamoDBClient](
    client: C,
    tableName: String,
    magnet: SchemaMagnet
  ): LocalDynamoPartial[String, C] =
    new LocalDynamoPartial(client, tableName, magnet, tableName)

  def withTable[C <: DynamoDBClient, V, PK](client: C, table: Table[_, V, PK]): LocalDynamoPartial[Table[C, V, PK], C] =
    new LocalDynamoPartial(client, table.tableName, schema(table.schema), table.withClient(client))

  def withTableRK[C <: DynamoDBClient, V, PK, RK](
    client: C,
    table: TableWithRange[_, V, PK, RK]
  ): LocalDynamoPartial[TableWithRange[C, V, PK, RK], C] =
    new LocalDynamoPartial(client, table.tableName, schema(table.schema), table.withClient(client))

  def withRandomTable[C <: DynamoDBClient](client: C, magnet: SchemaMagnet): LocalDynamoPartial[String, C] = {
    val tableName = UUID.randomUUID().toString
    withTable(client, tableName, magnet)
  }

  def withRandomTable[C <: DynamoDBClient, V](
    client: C
  )(implicit V: TableSchema[V]): LocalDynamoPartial[Table[C, V, V.IndexType], C] = {
    val tableName = UUID.randomUUID().toString
    withTable(client, V.withName(tableName))
  }

  def withRandomTableRK[C <: DynamoDBClient, V](
    client: C
  )(implicit V: TableSchemaWithRange[V]): LocalDynamoPartial[TableWithRange[C, V, V.PK, V.RK], C] = {
    val tableName = UUID.randomUUID().toString
    withTableRK(client, V.withName(tableName))
  }

  def schema[T](implicit T: TableSchema[T]): SchemaMagnet = new SchemaMagnet {
    private val first :: rest = T.schema

    override def hashKey: String = first._1
    override def rangeKey: Option[String] = rest.headOption.map(_._1)
    override def attributes: List[(String, ScalarType)] = T.schema
  }
}
