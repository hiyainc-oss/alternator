package com.hiya.alternator.aws2

 import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BenchTest extends AnyFunSpec with Matchers {
  val test = new Aws2Benchmark()

  it("should work") {
    val Right(data) = test.read_1000
    data shouldBe test.data1000
  }

}
