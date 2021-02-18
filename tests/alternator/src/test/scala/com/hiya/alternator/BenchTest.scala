package com.hiya.alternator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BenchTest extends AnyFunSpec with Matchers {
  val test = new AlternatorBenchmark()
  it("should work") {
    val Right(data) = test.read_10000
    data shouldBe test.data10000
  }
}
