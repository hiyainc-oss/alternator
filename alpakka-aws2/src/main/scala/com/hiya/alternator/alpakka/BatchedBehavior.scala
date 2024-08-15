package com.hiya.alternator.alpakka

trait BatchedBehavior {
  def queueSize: Int
  def inflight: Int
}
