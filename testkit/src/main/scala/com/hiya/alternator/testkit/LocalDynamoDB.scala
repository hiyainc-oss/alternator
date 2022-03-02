package com.hiya.alternator.testkit

import com.hiya.alternator.TableSchema
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbBaseClientBuilder}

import java.net.URI
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.DurationConverters._

object LocalDynamoDB {
  val DEFAULT_PORT = 8000
  val configuredPort: Int =
    Option(System.getProperty("dynamoDBLocalPort")).map(_.toInt).getOrElse(DEFAULT_PORT)

  def clientConfig[B <: DynamoDbBaseClientBuilder[B, _]](builder: B, port: Int = configuredPort): B =
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
      .region(Region.US_EAST_1)

  def client(port: Int = configuredPort): DynamoDbAsyncClient =
    clientConfig(DynamoDbAsyncClient.builder, port)
      .httpClient(NettyNioAsyncHttpClient.builder.build)
      .build

  def createTable(client: DynamoDbAsyncClient)(tableName: String)(magnet: SchemaMagnet): Future[CreateTableResponse] =
    client
      .createTable(
        CreateTableRequest.builder
          .attributeDefinitions(magnet.attributes.asJava)
          .tableName(tableName)
          .keySchema(magnet.keys.asJava)
          .provisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
          .build
      ).toScala

  def deleteTable(client: DynamoDbAsyncClient)(tableName: String): Future[DeleteTableResponse] =
    client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).toScala

  def withTable(client: DynamoDbAsyncClient)(tableName: String)(magnet: SchemaMagnet): WithTable =
    new WithTable(client, tableName, magnet)

  def withRandomTable(client: DynamoDbAsyncClient)(magnet: SchemaMagnet): WithRandomTable = {
    val tableName = UUID.randomUUID().toString
    new WithRandomTable(client, tableName, magnet)
  }

  def schema[T](implicit T : TableSchema[T]): SchemaMagnet = new SchemaMagnet {
    override def keys: List[KeySchemaElement] = {
      val first :: rest = T.schema
      KeySchemaElement.builder.attributeName(first._1).keyType(KeyType.HASH).build :: rest.map { case (name, _) =>
        KeySchemaElement.builder.attributeName(name).keyType(KeyType.RANGE).build
      }
    }

    override def attributes: List[AttributeDefinition] = {
      T.schema.map { case (name, fieldType) =>
        AttributeDefinition.builder.attributeName(name).attributeType(fieldType).build
      }
    }

  }



  private val arbitraryThroughputThatIsIgnoredByDynamoDBLocal =
    ProvisionedThroughput.builder.readCapacityUnits(1L).writeCapacityUnits(1L).build
}
