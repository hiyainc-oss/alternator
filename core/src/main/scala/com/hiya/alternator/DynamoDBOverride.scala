package com.hiya.alternator

import cats.kernel.Monoid

trait DynamoDBOverride[C <: DynamoDBClient, T] {
  def apply(ov: T): DynamoDBOverride.Applicator[C]
}

object DynamoDBOverride {

  def apply[C <: DynamoDBClient, T](implicit ev: DynamoDBOverride[C, T]): DynamoDBOverride[C, T] = ev

  trait Configure[B] {
    def apply[B1 <: B](builder: B1): B1
  }

  trait Applicator[C <: DynamoDBClient] {
    def apply(client: C): Configure[client.OverrideBuilder]
  }

  object Applicator {
    implicit def applicatorIsOverride[C <: DynamoDBClient]: DynamoDBOverride[C, Applicator[C]] = identity

    implicit def applicatorMonoid[C <: DynamoDBClient]: Monoid[Applicator[C]] = new Monoid[Applicator[C]] {
      override def combine(x: Applicator[C], y: Applicator[C]): Applicator[C] = new Applicator[C] {
        override def apply(client: C) = new Configure[client.OverrideBuilder] {
          override def apply[B1 <: client.OverrideBuilder](builder: B1) = {
            val b1 = x(client).apply(builder)
            y(client).apply(b1)
          }
        }
      }

      override val empty: Applicator[C] = new Applicator[C] {
        override def apply(client: C) = new Configure[client.OverrideBuilder] {
          override def apply[B1 <: client.OverrideBuilder](builder: B1) = builder
        }
      }
    }
  }

  implicit def overrideIsApplicator[C <: DynamoDBClient, O: DynamoDBOverride[C, *]](ov: O): Applicator[C] =
    ov.overrides[C]

  implicit class OverrideOps[O](val ov: O) extends AnyVal {
    def overrides[C <: DynamoDBClient](implicit ev: DynamoDBOverride[C, O]): DynamoDBOverride.Applicator[C] = ev(ov)
  }

  sealed trait Empty
  object Empty extends Empty {
    implicit def emptyOverride[C <: DynamoDBClient, E <: Empty]: DynamoDBOverride[C, E] = new DynamoDBOverride[C, E] {
      override def apply(ov: E): Applicator[C] = Monoid[Applicator[C]].empty
    }
  }
}
