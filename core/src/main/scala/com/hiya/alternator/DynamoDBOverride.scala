package com.hiya.alternator


trait DynamoDBOverride[C <: DynamoDBClient, T] {
  def apply(ov: T)(implicit client: C): client.OverrideBuilder
}

object DynamoDBOverride {
  def apply[C <: DynamoDBClient, T](implicit ev: DynamoDBOverride[C, T]): DynamoDBOverride[C, T] = ev

  sealed trait Empty
  object Empty extends Empty {
    implicit def emptyOverride[C <: DynamoDBClient, E <: Empty]: DynamoDBOverride[C, E] = new DynamoDBOverride[C, E] {
      override def apply(ov: E)(implicit client: C): client.OverrideBuilder = identity[client.Override]
    }
  }

  final case class CombinedOverride[C <: DynamoDBClient, O1, O2](ov1: O1, ov2: O2)
  object CombinedOverride {
    implicit def combinedOverride[C <: DynamoDBClient, O1: DynamoDBOverride[C, *], O2: DynamoDBOverride[C, *]]: DynamoDBOverride[C, CombinedOverride[C, O1, O2]]= 
      new DynamoDBOverride[C, CombinedOverride[C, O1, O2]] {
      override def apply(ov: CombinedOverride[C, O1, O2])(implicit client: C): client.OverrideBuilder = {
        val f1 = DynamoDBOverride[C, O1].apply(ov.ov1)
        val f2 = DynamoDBOverride[C, O2].apply(ov.ov2)
        f1.andThen(f2)
      }
    }
  }

  implicit class OverrideOps[C <: DynamoDBClient, O](val ov: O) extends AnyVal {
    def andThen[O2](ov2: O2) = CombinedOverride[C, O, O2](ov, ov2)
  }









}
