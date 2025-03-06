package com.hiya.alternator.aws2.internal

import cats.Monoid
import com.hiya.alternator.DynamoDBClient
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class Aws2DynamoDBClient(val underlying: DynamoDbAsyncClient) extends DynamoDBClient {
  type Client = Aws2DynamoDBClient.Client
  type Override = Aws2DynamoDBClient.Override
  val Override: Monoid[Override] = cats.instances.function.catsKernelMonoidForFunction1
}

object Aws2DynamoDBClient {
  type Client = DynamoDbAsyncClient
  type Override = AwsRequestOverrideConfiguration.Builder => AwsRequestOverrideConfiguration.Builder


  implicit object HasOverride extends DynamoDBClient.HasOverride[Aws2DynamoDBClient, Override] {
    override def resolve(ov: Override)(implicit client: Aws2DynamoDBClient): Override = ov
  }
}
