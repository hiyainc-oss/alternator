package com.hiya.alternator

trait DynamoDBClient {
  type Client
  type Override
  type OverrideBuilder = Override => Override
} 

object DynamoDBClient {
  
  sealed trait Missing extends DynamoDBClient {
    type Client = Nothing
    type Override = Unit
  }

  object Missing extends Missing



}



