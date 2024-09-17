package com.hiya.alternator

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder, model}
import com.hiya.alternator.internal.ConditionalSupport
import com.hiya.alternator.schema.{AttributeValue, ScalarType}
import com.hiya.alternator.testkit.LocalDynamoClient

import java.nio.ByteBuffer
import java.util.{Collection => JCollection, List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

package object aws1 {
  type Aws1TableLike[V, PK] = TableLike[AmazonDynamoDBAsync, V, PK]
  type Aws1TableWithRangeKeyLike[V, PK, RK] = TableWithRangeKeyLike[AmazonDynamoDBAsync, V, PK, RK]
  type Aws1DynamoDB[F[_]] = DynamoDB.Client[F, AmazonDynamoDBAsync]

  implicit object PutIsConditional extends ConditionalSupport[model.PutItemRequest, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.PutItemRequest,
      conditionExpression: String
    ): model.PutItemRequest =
      builder.withConditionExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.PutItemRequest,
      attributeNames: JMap[String, String]
    ): model.PutItemRequest =
      builder.withExpressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.PutItemRequest,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.PutItemRequest =
      builder.withExpressionAttributeValues(attributeValues)
  }

  implicit object DeleteIsConditional extends ConditionalSupport[model.DeleteItemRequest, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.DeleteItemRequest,
      conditionExpression: String
    ): model.DeleteItemRequest =
      builder.withConditionExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.DeleteItemRequest,
      attributeNames: JMap[String, String]
    ): model.DeleteItemRequest =
      builder.withExpressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.DeleteItemRequest,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.DeleteItemRequest =
      builder.withExpressionAttributeValues(attributeValues)
  }

  implicit object ScanIsConditional extends ConditionalSupport[model.ScanRequest, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.ScanRequest,
      conditionExpression: String
    ): model.ScanRequest =
      builder.withFilterExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.ScanRequest,
      attributeNames: JMap[String, String]
    ): model.ScanRequest =
      builder.withExpressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.ScanRequest,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.ScanRequest =
      builder.withExpressionAttributeValues(attributeValues)
  }

  implicit object QueryIsConditional extends ConditionalSupport[model.QueryRequest, model.AttributeValue] {
    override def withConditionExpression(
      builder: model.QueryRequest,
      conditionExpression: String
    ): model.QueryRequest =
      builder.withKeyConditionExpression(conditionExpression)

    override def withExpressionAttributeNames(
      builder: model.QueryRequest,
      attributeNames: JMap[String, String]
    ): model.QueryRequest =
      builder.withExpressionAttributeNames(attributeNames)

    override def withExpressionAttributeValues(
      builder: model.QueryRequest,
      attributeValues: JMap[String, model.AttributeValue]
    ): model.QueryRequest =
      builder.withExpressionAttributeValues(attributeValues)
  }

  implicit object Aws1LocalDynamoClient extends LocalDynamoClient[AmazonDynamoDBAsync] {
    type Config = AmazonDynamoDBAsyncClientBuilder

    def config(builder: AmazonDynamoDBAsyncClientBuilder, port: Int): AmazonDynamoDBAsyncClientBuilder =
      builder
        .withCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "credentials"))
        )
        .withEndpointConfiguration(
          new EndpointConfiguration(s"http://localhost:$port", "us-east-1")
        )

    override def config(port: Int): AmazonDynamoDBAsyncClientBuilder =
      config(AmazonDynamoDBAsyncClientBuilder.standard(), port)

    override def client(port: Int): AmazonDynamoDBAsync =
      config(AmazonDynamoDBAsyncClientBuilder.standard(), port).build
  }

  implicit def typeOf(t: ScalarType): model.ScalarAttributeType = t match {
    case ScalarType.String => model.ScalarAttributeType.S
    case ScalarType.Numeric => model.ScalarAttributeType.N
    case ScalarType.Binary => model.ScalarAttributeType.B
  }

  implicit object Aws1IsAttributeValues extends AttributeValue[model.AttributeValue] {
    override def map(av: model.AttributeValue): Option[JMap[String, model.AttributeValue]] =
      Option(av.getM)

    override def createMap(map: JMap[String, model.AttributeValue]): model.AttributeValue =
      new model.AttributeValue().withM(map)

    override val nullValue: model.AttributeValue =
      new model.AttributeValue().withNULL(true)

    override def isNull(av: model.AttributeValue): Boolean =
      av.isNULL

    override def string(av: model.AttributeValue): Option[String] =
      Option(av.getS)

    override def createString(s: String): model.AttributeValue =
      new model.AttributeValue().withS(s)

    override def bool(av: model.AttributeValue): Option[Boolean] =
      Option(av.getBOOL).map(Boolean.unbox)

    override val trueValue: model.AttributeValue =
      new model.AttributeValue().withBOOL(true)

    override val falseValue: model.AttributeValue =
      new model.AttributeValue().withBOOL(false)

    override def list(av: model.AttributeValue): Option[JList[model.AttributeValue]] =
      Option(av.getL)

    override def createList(av: JList[model.AttributeValue]): model.AttributeValue =
      new model.AttributeValue().withL(av)

    override val emptyList: model.AttributeValue =
      new model.AttributeValue().withL()

    override def stringSet(av: model.AttributeValue): Option[JCollection[String]] =
      Option(av.getSS)

    override def createStringSet(value: JCollection[String]): model.AttributeValue =
      new model.AttributeValue().withSS(value)

    override def createNumberSet(value: JCollection[String]): model.AttributeValue =
      new model.AttributeValue().withNS(value)

    override def numberSet(av: model.AttributeValue): Option[JCollection[String]] =
      Option(av.getNS)

    override def createBinary(value: Array[Byte]): model.AttributeValue =
      new model.AttributeValue().withB(ByteBuffer.wrap(value))

    override def createBinary(value: ByteBuffer): model.AttributeValue =
      new model.AttributeValue().withB(value)

    override def byteBuffer(av: model.AttributeValue): Option[ByteBuffer] =
      Option(av.getB)

    override def byteArray(av: model.AttributeValue): Option[Array[Byte]] =
      Option(av.getB).map(_.array())

    override def createByteBufferSet(value: scala.Iterable[ByteBuffer]): model.AttributeValue =
      new model.AttributeValue().withBS(value.asJavaCollection)

    override def byteBufferSet(av: model.AttributeValue): Option[Iterable[ByteBuffer]] =
      Option(av.getBS).map(_.asScala.view)

    override def numeric(av: model.AttributeValue): Option[String] =
      Option(av.getN)

    override def createNumeric(value: String): model.AttributeValue =
      new model.AttributeValue().withN(value)
  }
}
