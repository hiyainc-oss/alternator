package com.hiya.alternator

import cats.Monoid


trait DynamoDBClient {
  type Client
  type Override
  val Override: Monoid[Override]
} 

object DynamoDBClient {
  
  sealed trait Missing extends DynamoDBClient {
    type Client = Nothing
    type Override = Unit
    override val Override: Monoid[Unit] = cats.instances.unit.catsKernelStdAlgebraForUnit
  }
  object Missing extends Missing


  trait HasOverride[C <: DynamoDBClient, O] {
    def resolve(ov: O)(implicit client: C): client.Override

  }

  object HasOverride {
    def apply[C <: DynamoDBClient, O](implicit ev: HasOverride[C, O]): HasOverride[C, O] = ev

    implicit def forNothing[C <: DynamoDBClient]: HasOverride[C, Nothing] = new HasOverride[C, Nothing] {
      override def resolve(ov: Nothing)(implicit client: C): client.Override = ov
    }
  }
}
