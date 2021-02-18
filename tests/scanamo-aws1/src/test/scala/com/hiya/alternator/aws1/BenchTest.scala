package com.hiya.alternator.aws1

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BenchTest extends AnyFunSpec with Matchers  {
  val test = new Aws1Benchmark()
  it ("should work") {
    val Right(data) = test.read_10000
    data shouldBe test.data10000
  }
}
