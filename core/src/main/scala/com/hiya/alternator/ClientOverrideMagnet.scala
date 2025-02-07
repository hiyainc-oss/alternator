package com.hiya.alternator

abstract class ClientOverrideMagnet[F[_], T](val DB: DynamoDB.Client[F, T]) {
  def overrides: DB.Overrides
}

trait ClientConverter[T1, T2] {
  def apply(v: T1): T2
}

case class ClientOverrides[O](overrides: O) extends AnyVal {
  implicit def toMagnet[F[_], C](implicit
    DB: DynamoDB.Client[F, C] { type Overrides = O }
  ): ClientOverrideMagnet[F, C] =
    new ClientOverrideMagnet(DB) {
      override def overrides: this.DB.Overrides = ClientOverrides.this.overrides.asInstanceOf[this.DB.Overrides]
    }
}
