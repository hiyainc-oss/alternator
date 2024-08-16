package com.hiya.alternator

trait SchedulerMetrics {
  def queueSize: Int
  def inflight: Int
}

trait BatchMonitoring[-PK] {
  def register(actorName: String, behavior: SchedulerMetrics): Unit
  def retries(actorName: String, failed: List[PK]): Unit
  def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit
  def close(): Unit
}

object BatchMonitoring {
  object Disabled extends BatchMonitoring[Any] {
    override def register(actorName: String, behavior: SchedulerMetrics): Unit = ()
    override def retries(actorName: String, failed: List[Any]): Unit = ()
    override def requestComplete(actorName: String, ex: Option[Throwable], keys: List[Any], durationNano: Long): Unit =
      ()
    override def close(): Unit = {}
  }
}
