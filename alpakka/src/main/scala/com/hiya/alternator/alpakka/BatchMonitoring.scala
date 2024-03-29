package com.hiya.alternator.alpakka

import com.hiya.alternator.Table.PK


trait BatchMonitoring {
  def register(actorName: String, behavior: BatchedBehavior): Unit
  def retries(actorName: String, failed: List[PK]): Unit
  def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit
  def close(): Unit
}

object BatchMonitoring {
  object Disabled extends BatchMonitoring {
    override def register(actorName: String, behavior: BatchedBehavior): Unit = ()
    override def retries(actorName: String, failed: List[PK]): Unit = ()
    override def requestComplete(actorName: String, ex: Option[Throwable], keys: List[PK], durationNano: Long): Unit = ()
    override def close(): Unit = {}
  }
}
