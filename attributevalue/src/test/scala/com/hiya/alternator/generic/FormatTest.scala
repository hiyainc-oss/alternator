package com.hiya.alternator.generic

import com.hiya.alternator.DynamoFormat
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class FormatTest extends AnyFunSpec with Matchers {
  private def iso[T](orig: T)(implicit T: DynamoFormat[T]): Assertion = {
    T.read(T.write(orig)) shouldBe Right(orig)
  }

  describe("semiauto") {
    it("should derive a string") {
      iso("asd")
    }

    it("should derive a case class") {
      final case class Test(a: String)
      iso(Test("asd"))(semiauto.deriveCompound[Test])
    }

    it("should derive a case class [int]") {
      import auto._
      final case class Test(a: Int)
      iso(Test(1))
    }

    it("should derive a sealed trait") {
      import auto._

      sealed abstract class A
      final case class B(s: String) extends A
      final case object C extends A

      iso[A](B("asd"))
      iso[A](C)
    }
  }
}
