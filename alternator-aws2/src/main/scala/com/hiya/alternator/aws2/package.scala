package com.hiya.alternator

import com.hiya.alternator.internal.ConditionalSupport
import com.hiya.alternator.schema.{AttributeValue, ScalarType}
import com.hiya.alternator.testkit.LocalDynamoClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{
  DynamoDbAsyncClient,
  DynamoDbAsyncClientBuilder,
  DynamoDbBaseClientBuilder,
  model
}

import java.net.URI
import java.nio.ByteBuffer
import java.util.{Collection => JCollection, List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

package object aws2 {
  implicit object PutIsConditional extends ConditionalSupport[model.PutItemRequest.Builder, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.PutItemRequest.Builder,
      conditionExpression: String
    ): model.PutItemRequest.Builder = builder.conditionExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.PutItemRequest.Builder,
      attributeNames: JMap[String, String]
    ): model.PutItemRequest.Builder = builder.expressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.PutItemRequest.Builder,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.PutItemRequest.Builder = builder.expressionAttributeValues(attributeValues)
  }

  implicit object DeleteIsConditional
    extends ConditionalSupport[model.DeleteItemRequest.Builder, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.DeleteItemRequest.Builder,
      conditionExpression: String
    ): model.DeleteItemRequest.Builder = builder.conditionExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.DeleteItemRequest.Builder,
      attributeNames: JMap[String, String]
    ): model.DeleteItemRequest.Builder = builder.expressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.DeleteItemRequest.Builder,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.DeleteItemRequest.Builder = builder.expressionAttributeValues(attributeValues)
  }

  implicit object Aws2LocalDynamoClient extends LocalDynamoClient[DynamoDbAsyncClient] {
    type Config = DynamoDbAsyncClientBuilder

    def config[B <: DynamoDbBaseClientBuilder[B, _]](builder: B, port: Int): B =
      builder
        .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "credentials"))
        )
        .endpointOverride(URI.create(s"http://localhost:$port"))
        .region(Region.US_EAST_1)

    override def config(port: Int): DynamoDbAsyncClientBuilder =
      config(DynamoDbAsyncClient.builder, port)

    override def client(port: Int): DynamoDbAsyncClient =
      config(port)
        .httpClient(NettyNioAsyncHttpClient.builder.build)
        .build
  }

  implicit object ScanIsConditional extends ConditionalSupport[model.ScanRequest.Builder, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.ScanRequest.Builder,
      conditionExpression: String
    ): model.ScanRequest.Builder = builder.filterExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.ScanRequest.Builder,
      attributeNames: JMap[String, String]
    ): model.ScanRequest.Builder = builder.expressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.ScanRequest.Builder,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.ScanRequest.Builder = builder.expressionAttributeValues(attributeValues)
  }

  implicit object QueryIsConditional extends ConditionalSupport[model.QueryRequest.Builder, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.QueryRequest.Builder,
      conditionExpression: String
    ): model.QueryRequest.Builder = builder.keyConditionExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.QueryRequest.Builder,
      attributeNames: JMap[String, String]
    ): model.QueryRequest.Builder = builder.expressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.QueryRequest.Builder,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.QueryRequest.Builder = builder.expressionAttributeValues(attributeValues)
  }

  implicit def typeOf(t: ScalarType): model.ScalarAttributeType = t match {
    case ScalarType.String => model.ScalarAttributeType.S
    case ScalarType.Numeric => model.ScalarAttributeType.N
    case ScalarType.Binary => model.ScalarAttributeType.B
  }

  implicit object Aws2IsAttributeValues extends AttributeValue[model.AttributeValue] {
    override def map(av: model.AttributeValue): Option[JMap[String, model.AttributeValue]] =
      if (av.hasM) Option(av.m()) else None

    override def createMap(map: JMap[String, model.AttributeValue]): model.AttributeValue =
      model.AttributeValue.fromM(map)

    override val nullValue: model.AttributeValue =
      model.AttributeValue.fromNul(true)

    override def isNull(av: model.AttributeValue): Boolean =
      av.nul()

    override def string(av: model.AttributeValue): Option[String] =
      Option(av.s())

    override def createString(s: String): model.AttributeValue =
      model.AttributeValue.fromS(s)

    override def bool(av: model.AttributeValue): Option[Boolean] =
      Option(av.bool()).map(Boolean.unbox)

    override val trueValue: model.AttributeValue =
      model.AttributeValue.fromBool(true)

    override val falseValue: model.AttributeValue =
      model.AttributeValue.fromBool(false)

    override def list(av: model.AttributeValue): Option[JList[model.AttributeValue]] =
      if (av.hasL) Option(av.l()) else None

    override def createList(av: JList[model.AttributeValue]): model.AttributeValue =
      model.AttributeValue.fromL(av)

    override val emptyList: model.AttributeValue =
      model.AttributeValue.fromL(JList.of())

    override def stringSet(av: model.AttributeValue): Option[JCollection[String]] =
      if (av.hasSs) Option(av.ss()) else None

    override def createStringSet(value: JCollection[String]): model.AttributeValue =
      model.AttributeValue.builder().ss(value).build()

    override def createNumberSet(value: JCollection[String]): model.AttributeValue =
      model.AttributeValue.builder().ns(value).build()

    override def numberSet(av: model.AttributeValue): Option[JCollection[String]] =
      if (av.hasNs) Option(av.ns()) else None

    override def createBinary(value: Array[Byte]): model.AttributeValue =
      model.AttributeValue.fromB(SdkBytes.fromByteArrayUnsafe(value))

    override def createBinary(value: ByteBuffer): model.AttributeValue =
      model.AttributeValue.fromB(SdkBytes.fromByteBuffer(value))

    override def byteBuffer(av: model.AttributeValue): Option[ByteBuffer] =
      Option(av.b()).map(_.asByteBuffer())

    override def byteArray(av: model.AttributeValue): Option[Array[Byte]] =
      Option(av.b()).map(_.asByteArrayUnsafe())

    override def numeric(av: model.AttributeValue): Option[String] =
      Option(av.n())

    override def createNumeric(value: String): model.AttributeValue =
      model.AttributeValue.fromN(value)

    override def createByteBufferSet(value: Iterable[ByteBuffer]): model.AttributeValue =
      model.AttributeValue.builder().bs(value.view.map(SdkBytes.fromByteBuffer).asJavaCollection).build()

    override def byteBufferSet(av: model.AttributeValue): Option[Iterable[ByteBuffer]] =
      if (av.hasBs) Option(av.bs()).map(_.asScala.map(_.asByteBuffer())) else None
  }

}
