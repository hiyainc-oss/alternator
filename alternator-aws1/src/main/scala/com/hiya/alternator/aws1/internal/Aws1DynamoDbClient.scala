package com.hiya.alternator.aws1.internal

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.hiya.alternator._

case class Aws1DynamoDBClient(val underlying: AmazonDynamoDBAsync) extends DynamoDBClient {
  type Client = Aws1DynamoDBClient.Client
  type Override = Aws1DynamoDBClient.Override
}

object Aws1DynamoDBClient {
  type Client = AmazonDynamoDBAsync
  type Override = Unit
  type OverrideBuilder = Override => Override

  implicit val hasOverride: DynamoDBClient.HasOverride[Aws1DynamoDBClient, Override] = new DynamoDBClient.HasOverride[Aws1DynamoDBClient, Override] {
    override def resolve(ov: OverrideBuilder)(implicit client: Aws1DynamoDBClient): OverrideBuilder = ov
  }
}
