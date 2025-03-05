package com.hiya.alternator.aws2.internal

import com.hiya.alternator.DynamoDBClient
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class Aws2DynamoDBClient(val underlying: DynamoDbAsyncClient) extends DynamoDBClient {
  type Client = Aws2DynamoDBClient.Client
  type Override = Aws2DynamoDBClient.Override
}

object Aws2DynamoDBClient {
  type Client = DynamoDbAsyncClient
  type Override = AwsRequestOverrideConfiguration


  implicit object HasOverride extends DynamoDBClient.HasOverride[Aws2DynamoDBClient, AwsRequestOverrideConfiguration] {
    override def resolve(ov: AwsRequestOverrideConfiguration)(implicit client: Aws2DynamoDBClient): AwsRequestOverrideConfiguration = ov
  }
}
