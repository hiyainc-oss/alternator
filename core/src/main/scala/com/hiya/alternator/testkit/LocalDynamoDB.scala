package com.hiya.alternator.testkit

import com.hiya.alternator.schema._

import java.util.UUID

trait LocalDynamoClient[C] {
  type Config

  def config(port: Int): Config
  def client(port: Int): C
}

object LocalDynamoDB {
  val DEFAULT_PORT = 8000
  val configuredPort: Int =
    Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(DEFAULT_PORT)

  def config[C](port: Int = configuredPort)(implicit localDynamoClient: LocalDynamoClient[C]): localDynamoClient.Config =
    localDynamoClient.config(port)

  def client[C](port: Int = configuredPort)(implicit localDynamoClient: LocalDynamoClient[C]): C =
    localDynamoClient.client(port)

  def withTable[C](client: C)(tableName: String, magnet: SchemaMagnet): LocalDynamoPartial[C] =
    new LocalDynamoPartial[C](client, tableName, magnet)

  def withRandomTable[C](client: C)(magnet: SchemaMagnet): LocalDynamoPartial[C] =
    withTable(client)(UUID.randomUUID().toString, magnet)

  def schema[T](implicit T: TableSchema[T]): SchemaMagnet = new SchemaMagnet {
    private val first :: rest = T.schema

    override def hashKey: String = first._1
    override def rangeKey: Option[String] = rest.headOption.map(_._1)
    override def attributes: List[(String, ScalarType)] = T.schema
  }
}
