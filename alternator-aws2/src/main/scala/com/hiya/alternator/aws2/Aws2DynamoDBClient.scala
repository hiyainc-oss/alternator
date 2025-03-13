package com.hiya.alternator.aws2

import com.hiya.alternator.DynamoDBClient
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class Aws2DynamoDBClient(val client: DynamoDbAsyncClient) extends DynamoDBClient {
  type Client = Aws2DynamoDBClient.Client
  type OverrideBuilder = Aws2DynamoDBClient.OverrideBuilder
}

object Aws2DynamoDBClient {
  type Client = DynamoDbAsyncClient
  type OverrideBuilder = AwsRequestOverrideConfiguration.Builder
}
