package aws2.alpakka

import com.hiya.alternator.aws2

trait BatchedBehavior {
  def queueSize: Int

  def inflight: Int
}
