package com.hiya.alternator

trait BatchedBehavior {
  def queueSize: Int
  def inflight: Int
}
