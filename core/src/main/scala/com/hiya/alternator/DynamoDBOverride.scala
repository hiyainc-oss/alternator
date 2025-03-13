package com.hiya.alternator

import cats.kernel.Monoid

trait DynamoDBOverride[C <: DynamoDBClient] {
  def apply(client: C): DynamoDBOverride.Configure[client.OverrideBuilder]
}

object DynamoDBOverride {
  def apply[C <: DynamoDBClient, O](ov: O)(implicit ev: O => DynamoDBOverride[C]): DynamoDBOverride[C] = ev(ov)

  trait Configure[B] { def apply(builder: B): B }

  implicit def monoid[C <: DynamoDBClient]: Monoid[DynamoDBOverride[C]] = new Monoid[DynamoDBOverride[C]] {
    override def combine(x: DynamoDBOverride[C], y: DynamoDBOverride[C]): DynamoDBOverride[C] =
      new DynamoDBOverride[C] {
        override def apply(client: C) = new Configure[client.OverrideBuilder] {
          override def apply(builder: client.OverrideBuilder) = {
            val b1 = x(client).apply(builder)
            y(client).apply(b1)
          }
        }
      }

    override val empty: DynamoDBOverride[C] = new DynamoDBOverride[C] {
      override def apply(client: C) = new Configure[client.OverrideBuilder] {
        override def apply(builder: client.OverrideBuilder) = builder
      }
    }
  }

  def empty[C <: DynamoDBClient]: DynamoDBOverride[C] = monoid[C].empty
}
