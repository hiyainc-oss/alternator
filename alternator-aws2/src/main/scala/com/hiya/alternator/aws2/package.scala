package com.hiya.alternator

import com.hiya.alternator.schema.{AttributeValue, ScalarType}
import com.hiya.alternator.testkit.LocalDynamoClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbBaseClientBuilder, model}

import java.net.URI
import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap, Set => JSet}

package object aws2 {
  implicit val aws2LocalDynamoClient: LocalDynamoClient[DynamoDbAsyncClient] =
    new LocalDynamoClient[DynamoDbAsyncClient] {
      def clientConfig[B <: DynamoDbBaseClientBuilder[B, _]](builder: B, port: Int): B =
        builder
          .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "credentials"))
          )
          .endpointOverride(URI.create(s"http://localhost:$port"))
          .region(Region.US_EAST_1)

      def client(port: Int): DynamoDbAsyncClient =
        clientConfig(DynamoDbAsyncClient.builder, port)
          .httpClient(NettyNioAsyncHttpClient.builder.build)
          .build
    }

  implicit def typeOf(t: ScalarType): model.ScalarAttributeType = t match {
    case ScalarType.String => model.ScalarAttributeType.S
    case ScalarType.Numeric => model.ScalarAttributeType.N
    case ScalarType.Binary => model.ScalarAttributeType.B
  }

  implicit val aws2IsAttributeValues: AttributeValue[model.AttributeValue] = new AttributeValue[model.AttributeValue] {
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

    override def stringSet(av: model.AttributeValue): Option[JList[String]] =
      if (av.hasSs) Option(av.ss()) else None

    override def createStringSet(value: JSet[String]): model.AttributeValue =
      model.AttributeValue.builder().ss(value).build()

    override def createNumberSet(value: JSet[String]): model.AttributeValue =
      model.AttributeValue.builder().ns(value).build()

    override def numberSet(av: model.AttributeValue): Option[JList[String]] =
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
  }

}
