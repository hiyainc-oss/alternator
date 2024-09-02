package com.hiya.alternator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BenchTest extends AnyFunSpec with Matchers {
  val test = new AlternatorBenchmark()
  it("should read") {
    val Right(data) = test.read_1000
    data shouldBe test.data1000
  }

  it("should write") {
    val data = test.write_100
    data shouldBe test.av100
  }
}
