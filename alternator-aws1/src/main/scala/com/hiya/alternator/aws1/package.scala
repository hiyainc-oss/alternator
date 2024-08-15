package com.hiya.alternator

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder, model}
import com.hiya.alternator.schema.{AttributeValue, ScalarType}
import com.hiya.alternator.testkit.LocalDynamoClient

import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap, Set => JSet}

package object aws1 {
  implicit val aws1LocalDynamoClient: LocalDynamoClient[AmazonDynamoDBAsync] = new LocalDynamoClient[AmazonDynamoDBAsync] {
    def clientConfig(builder: AmazonDynamoDBAsyncClientBuilder, port: Int): AmazonDynamoDBAsyncClientBuilder =
      builder
        .withCredentials(
          new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "credentials"))
        )
        .withEndpointConfiguration(
          new EndpointConfiguration(s"http://localhost:$port", "us-east-1")
        )

    def client(port: Int): AmazonDynamoDBAsync =
      clientConfig(AmazonDynamoDBAsyncClientBuilder.standard(), port)
        .build
  }

  implicit def typeOf(t: ScalarType): model.ScalarAttributeType = t match {
    case ScalarType.String => model.ScalarAttributeType.S
    case ScalarType.Numeric => model.ScalarAttributeType.N
    case ScalarType.Binary => model.ScalarAttributeType.B
  }

  implicit val aws1IsAttributeValues: AttributeValue[model.AttributeValue] = new AttributeValue[model.AttributeValue] {
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

    override def stringSet(av: model.AttributeValue): Option[JList[String]] =
      Option(av.getSS)

    override def createStringSet(value: JSet[String]): model.AttributeValue =
      new model.AttributeValue().withSS(value)

    override def createNumberSet(value: JSet[String]): model.AttributeValue =
      new model.AttributeValue().withNS(value)

    override def numberSet(av: model.AttributeValue): Option[JList[String]] =
      Option(av.getNS)

    override def createBinary(value: Array[Byte]): model.AttributeValue =
      new model.AttributeValue().withB(ByteBuffer.wrap(value))

    override def createBinary(value: ByteBuffer): model.AttributeValue =
      new model.AttributeValue().withB(value)

    override def byteBuffer(av: model.AttributeValue): Option[ByteBuffer] =
      Option(av.getB)

    override def byteArray(av: model.AttributeValue): Option[Array[Byte]] =
      Option(av.getB).map(_.array())

    override def numeric(av: model.AttributeValue): Option[String] =
      Option(av.getN)

    override def createNumeric(value: String): model.AttributeValue =
      new model.AttributeValue().withN(value)
  }
}