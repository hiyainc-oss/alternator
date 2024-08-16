package com.hiya.alternator

import cats.Monad

trait SchedulerMetrics[F[_]] {
  def queueSize: F[Int]
  def inflight: F[Int]
}

trait BatchMonitoring[F[_], -PK] {
  def register(actorName: String, behavior: SchedulerMetrics[F]): F[Unit]
  def retries(actorName: String, failed: List[PK]): F[Unit]
  def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): F[Unit]
  def close(): F[Unit]
}

object BatchMonitoring {
  class Disabled[F[_]: Monad] extends BatchMonitoring[F, Any] {
    override def register(actorName: String, behavior: SchedulerMetrics[F]): F[Unit] = Monad[F].pure(())
    override def retries(actorName: String, failed: List[Any]): F[Unit] = Monad[F].pure(())
    override def requestComplete(
      actorName: String,
      ex: Option[Throwable],
      keys: List[Any],
      durationNano: Long
    ): F[Unit] = Monad[F].pure(())
    override def close(): F[Unit] = Monad[F].pure(())
  }
}
