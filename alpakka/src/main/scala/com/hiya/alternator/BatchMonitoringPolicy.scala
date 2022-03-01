package com.hiya.alternator

import com.hiya.alternator.Table.PK


trait BatchMonitoringPolicy {
  def inflightCount(actorName: String, value: () => Int): Unit
  def queueSizeGauge(actorName: String, value: () => Int): Unit
  def retries(actorName: String, failed: List[PK]): Unit
  def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit
}

object BatchMonitoringPolicy {
  object Disabled extends BatchMonitoringPolicy {
    override def inflightCount(actorName: String, value: () => Int): Unit = ()
    override def queueSizeGauge(actorName: String, value: () => Int): Unit = ()
    override def retries(actorName: String, failed: List[PK]): Unit = ()
    override def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit = ()
  }
}
