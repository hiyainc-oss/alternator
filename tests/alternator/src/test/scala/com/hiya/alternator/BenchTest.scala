package com.hiya.alternator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

object BenchTest {
  def main(args: Array[String]): Unit = {
    var i = 0
    while(i < 1000) {
      val test = new AlternatorBenchmark()
      test.write_1000
      i += 1
    }

  }
}

class BenchTest extends AnyFunSpec with Matchers {
  val test = new AlternatorBenchmark()
  it("should work") {
    test.write_1000
    val Right(data) = test.read_10000
    data shouldBe test.data10000
  }
}
