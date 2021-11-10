package com.hiya.alternator.util

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbBaseClientBuilder, DynamoDbClient}

import java.net.URI
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.compat.java8.DurationConverters._

object LocalDynamoDB {
  val DEFAULT_PORT = 8042

  def clientConfig[B <: DynamoDbBaseClientBuilder[B, _]](builder: B, port: Int = DEFAULT_PORT): B =
    builder
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "credentials"))
      )
      .endpointOverride(URI.create(s"http://localhost:$port"))
      .overrideConfiguration(
        ClientOverrideConfiguration.builder
          .apiCallAttemptTimeout(5.seconds.toJava)
          .apiCallTimeout(5.seconds.toJava)
          .build
      )
      .region(Region.EU_WEST_1)

  def client(port: Int = DEFAULT_PORT): DynamoDbAsyncClient =
    clientConfig(DynamoDbAsyncClient.builder, port)
      .httpClient(NettyNioAsyncHttpClient.builder.build)
      .build

  def syncClient(port: Int = DEFAULT_PORT): DynamoDbClient =
    clientConfig(DynamoDbClient.builder, port).build

  def createTable(
    client: DynamoDbAsyncClient
  )(tableName: String)(attributes: (String, ScalarAttributeType)*) =
    client
      .createTable(
        CreateTableRequest.builder
          .attributeDefinitions(attributeDefinitions(attributes))
          .tableName(tableName)
          .keySchema(keySchema(attributes))
          .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
          .build
      )
      .get

  def createTable(
    client: DynamoDbClient
  )(tableName: String)(attributes: (String, ScalarAttributeType)*) =
    client
      .createTable(
        CreateTableRequest.builder
          .attributeDefinitions(attributeDefinitions(attributes))
          .tableName(tableName)
          .keySchema(keySchema(attributes))
          .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
          .build
      )

  def createTableWithIndex(
    client: DynamoDbAsyncClient,
    tableName: String,
    secondaryIndexName: String,
    primaryIndexAttributes: List[(String, ScalarAttributeType)],
    secondaryIndexAttributes: List[(String, ScalarAttributeType)]
  ) =
    client
      .createTable(
        CreateTableRequest.builder
          .tableName(tableName)
          .attributeDefinitions(
            attributeDefinitions(
              primaryIndexAttributes ++ (secondaryIndexAttributes diff primaryIndexAttributes)
            )
          )
          .keySchema(keySchema(primaryIndexAttributes))
          .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
          .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder
              .indexName(secondaryIndexName)
              .keySchema(keySchema(secondaryIndexAttributes))
              .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
              .projection(Projection.builder.projectionType(ProjectionType.ALL).build)
              .build
          )
          .build
      )
      .get

  def createTableWithIndex(
    client: DynamoDbClient,
    tableName: String,
    secondaryIndexName: String,
    primaryIndexAttributes: List[(String, ScalarAttributeType)],
    secondaryIndexAttributes: List[(String, ScalarAttributeType)]
  ) =
    client
      .createTable(
        CreateTableRequest.builder
          .tableName(tableName)
          .attributeDefinitions(
            attributeDefinitions(
              primaryIndexAttributes ++ (secondaryIndexAttributes diff primaryIndexAttributes)
            )
          )
          .keySchema(keySchema(primaryIndexAttributes))
          .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
          .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder
              .indexName(secondaryIndexName)
              .keySchema(keySchema(secondaryIndexAttributes))
              .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
              .projection(Projection.builder.projectionType(ProjectionType.ALL).build)
              .build
          )
          .build
      )

  def deleteTable(client: DynamoDbAsyncClient)(tableName: String) =
    client.deleteTable { b => b.tableName(tableName); () }.get

  def deleteTable(client: DynamoDbClient)(tableName: String) =
    client.deleteTable { b => b.tableName(tableName); () }

  def withTable[T](client: DynamoDbAsyncClient)(tableName: String)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: => T
  ): T = {
    createTable(client)(tableName)(attributeDefinitions: _*)
    val res =
      try thunk
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def withTable[T](client: DynamoDbClient)(tableName: String)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: => T
  ): T = {
    createTable(client)(tableName)(attributeDefinitions: _*)
    val res =
      try thunk
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def withRandomTable[T](client: DynamoDbAsyncClient)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: String => T
  ): T = {
    var created: Boolean  = false
    var tableName: String = null
    while (!created)
      try {
        tableName = java.util.UUID.randomUUID.toString
        createTable(client)(tableName)(attributeDefinitions: _*)
        created = true
      } catch {
        case _: ResourceInUseException =>
      }

    val res =
      try thunk(tableName)
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def withRandomTable[T](client: DynamoDbClient)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: String => T
  ): T = {
    var created: Boolean  = false
    var tableName: String = null
    while (!created)
      try {
        tableName = java.util.UUID.randomUUID.toString
        createTable(client)(tableName)(attributeDefinitions: _*)
        created = true
      } catch {
        case _: ResourceInUseException =>
      }

    val res =
      try thunk(tableName)
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def usingTable[T](client: DynamoDbAsyncClient)(tableName: String)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: => T
  ): Unit = {
    withTable(client)(tableName)(attributeDefinitions: _*)(thunk)
    ()
  }

  def usingTable[T](client: DynamoDbClient)(tableName: String)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: => T
  ): Unit = {
    withTable(client)(tableName)(attributeDefinitions: _*)(thunk)
    ()
  }

  def usingRandomTable[T](client: DynamoDbAsyncClient)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: String => T
  ): Unit = {
    withRandomTable(client)(attributeDefinitions: _*)(thunk)
    ()
  }

  def usingRandomTable[T](client: DynamoDbClient)(
    attributeDefinitions: (String, ScalarAttributeType)*
  )(
    thunk: String => T
  ): Unit = {
    withRandomTable(client)(attributeDefinitions: _*)(thunk)
    ()
  }

  def withTableWithSecondaryIndex[T](
    client: DynamoDbAsyncClient
  )(tableName: String, secondaryIndexName: String)(
    primaryIndexAttributes: (String, ScalarAttributeType)*
  )(secondaryIndexAttributes: (String, ScalarAttributeType)*)(
    thunk: => T
  ): T = {
    createTableWithIndex(
      client,
      tableName,
      secondaryIndexName,
      primaryIndexAttributes.toList,
      secondaryIndexAttributes.toList
    )
    val res =
      try thunk
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def withTableWithSecondaryIndex[T](
    client: DynamoDbClient
  )(tableName: String, secondaryIndexName: String)(
    primaryIndexAttributes: (String, ScalarAttributeType)*
  )(secondaryIndexAttributes: (String, ScalarAttributeType)*)(
    thunk: => T
  ): T = {
    createTableWithIndex(
      client,
      tableName,
      secondaryIndexName,
      primaryIndexAttributes.toList,
      secondaryIndexAttributes.toList
    )
    val res =
      try thunk
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def withRandomTableWithSecondaryIndex[T](
    client: DynamoDbAsyncClient
  )(primaryIndexAttributes: (String, ScalarAttributeType)*)(
    secondaryIndexAttributes: (String, ScalarAttributeType)*
  )(
    thunk: (String, String) => T
  ): T = {
    var tableName: String = null
    var indexName: String = null
    var created: Boolean  = false
    while (!created)
      try {
        tableName = java.util.UUID.randomUUID.toString
        indexName = java.util.UUID.randomUUID.toString
        createTableWithIndex(
          client,
          tableName,
          indexName,
          primaryIndexAttributes.toList,
          secondaryIndexAttributes.toList
        )
        created = true
      } catch {
        case _: ResourceInUseException =>
      }

    val res =
      try thunk(tableName, indexName)
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  def withRandomTableWithSecondaryIndex[T](
    client: DynamoDbClient
  )(primaryIndexAttributes: (String, ScalarAttributeType)*)(
    secondaryIndexAttributes: (String, ScalarAttributeType)*
  )(
    thunk: (String, String) => T
  ): T = {
    var tableName: String = null
    var indexName: String = null
    var created: Boolean  = false
    while (!created)
      try {
        tableName = java.util.UUID.randomUUID.toString
        indexName = java.util.UUID.randomUUID.toString
        createTableWithIndex(
          client,
          tableName,
          indexName,
          primaryIndexAttributes.toList,
          secondaryIndexAttributes.toList
        )
        created = true
      } catch {
        case _: ResourceInUseException =>
      }

    val res =
      try thunk(tableName, indexName)
      finally {
        deleteTable(client)(tableName)
        ()
      }
    res
  }

  private def keySchema(attributes: Seq[(String, ScalarAttributeType)]) = {
    val hashKeyWithType :: rangeKeyWithType = attributes.toList
    val keySchemas                          =
      hashKeyWithType._1 -> KeyType.HASH :: rangeKeyWithType.map(_._1 -> KeyType.RANGE)
    keySchemas.map { case (symbol, keyType) =>
      KeySchemaElement.builder.attributeName(symbol).keyType(keyType).build
    }.asJava
  }

  private def attributeDefinitions(attributes: Seq[(String, ScalarAttributeType)]) =
    attributes.map { case (symbol, attributeType) =>
      AttributeDefinition.builder.attributeName(symbol).attributeType(attributeType).build
    }.asJava

  private val arbitraryThroughputThatIsIgnoredByDynamoDBLocal =
    ProvisionedThroughput.builder.readCapacityUnits(1L).writeCapacityUnits(1L).build
}
