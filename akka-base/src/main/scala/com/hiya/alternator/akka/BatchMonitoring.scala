package com.hiya.alternator.akka

trait SchedulerMetrics {
  def queueSize: Int
  def inflight: Int
}

trait BatchMonitoring[PK] {
  def register(actorName: String, behavior: SchedulerMetrics): Unit
  def retries(actorName: String, failed: List[PK]): Unit
  def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit
  def close(): Unit
}

object BatchMonitoring {
  object Disabled extends BatchMonitoring[Nothing] {
    override def register(actorName: String, behavior: SchedulerMetrics): Unit = ()
    override def retries(actorName: String, failed: List[Nothing]): Unit = ()
    override def requestComplete(actorName: String, ex: Option[Throwable], keys: List[Nothing], durationNano: Long): Unit = ()
    override def close(): Unit = {}
  }
}
