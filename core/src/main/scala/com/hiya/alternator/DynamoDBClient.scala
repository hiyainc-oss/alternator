package com.hiya.alternator

trait DynamoDBClient {
  type Client
  type OverrideBuilder
}

object DynamoDBClient {

  sealed trait Missing extends DynamoDBClient {
    type Client = Nothing
    type Override = Unit
  }

  object Missing extends Missing

}
