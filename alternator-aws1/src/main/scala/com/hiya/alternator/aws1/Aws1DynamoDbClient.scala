package com.hiya.alternator.aws1

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.hiya.alternator._

case class Aws1DynamoDBClient(val underlying: AmazonDynamoDBAsync) extends DynamoDBClient {
  type Client = Aws1DynamoDBClient.Client
  type OverrideBuilder = Aws1DynamoDBClient.OverrideBuilder
}

object Aws1DynamoDBClient {
  type Client = AmazonDynamoDBAsync
  type OverrideBuilder = AmazonWebServiceRequest
}
