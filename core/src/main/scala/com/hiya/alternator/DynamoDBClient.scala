package com.hiya.alternator

import cats.Monoid


trait DynamoDBClient {
  type Client
  type Override
  
  type OverrideBuilder = Override => Override

  val Override: Monoid[OverrideBuilder] = new Monoid[OverrideBuilder] {
    override def empty: OverrideBuilder = identity
    override def combine(x: OverrideBuilder, y: OverrideBuilder): OverrideBuilder = x.andThen(y)
  }
} 

object DynamoDBClient {
  
  sealed trait Missing extends DynamoDBClient {
    type Client = Nothing
    type Override = Unit
  }

  object Missing extends Missing


  trait HasOverride[C <: DynamoDBClient, O] {
    def resolve(ov: O => O)(implicit client: C): client.OverrideBuilder

  }

  object HasOverride {
    def apply[C <: DynamoDBClient, O](implicit ev: HasOverride[C, O]): HasOverride[C, O] = ev

    // implicit def forNothing[C <: DynamoDBClient]: HasOverride[C, Nothing] = new HasOverride[C, Nothing] {
    //   override def resolve(ov: Nothing)(implicit client: C): client.Override = ov
    // }
  }
}
