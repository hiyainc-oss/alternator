package com.hiya.alternator.generic

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

import java.nio.ByteBuffer
import com.hiya.alternator.ByteBufferSupport._


object CompatibilityTests {


  case object CO

  sealed trait S[+A]
  final case object SA extends S[Nothing]
  final case class SB[A](data: A) extends S[A]

  final case class P[A](data: A)

  sealed trait B
  final case class B0(i: Int, l: Long, bd: BigDecimal, f: Float, d: Double, s: String, b: Boolean, ba: ByteBuffer) extends B
  final case class B1(i: Option[Int], l: Option[Long], bd: Option[BigDecimal], f: Option[Float], d: Option[Double], s: Option[String], b: Option[Boolean], ba: Option[ByteBuffer]) extends B
  final case class B2(i: List[Int], l: List[Long], bd: List[BigDecimal], f: List[Float], d: List[Double], s: List[String], b: List[Boolean], ba: List[ByteBuffer]) extends B
  final case class B3(a: B, b: List[B], c: Option[B], d: Option[List[B]], e: List[Option[B]]) extends B

  val nonEmptyStringGen: Gen[String] =
    Gen.nonEmptyContainerOf[Array, Char](Arbitrary.arbChar.arbitrary).map(arr => new String(arr))


  object B {
    implicit val arbitraryB: Arbitrary[B] = Arbitrary(genB)
    def genB: Gen[B] = Gen.oneOf[B](gen0, gen1, gen2, gen3)
    def genB0: Gen[B] = Gen.oneOf[B](gen0, gen1, gen2)


    def gen0: Gen[B0] = for {
      i <- arbitrary[Int]
      l <- arbitrary[Long]
      bd <- arbitrary[BigDecimal]
      f <- arbitrary[Float]
      d <- arbitrary[Double]
      s <- nonEmptyStringGen
      b <- arbitrary[Boolean]
      ba <- arbitrary[ByteBuffer]
    } yield B0(i, l, bd, f, d, s, b, ba)

    def gen1: Gen[B1] = for {
      i <- arbitrary[Option[Int]]
      l <- arbitrary[Option[Long]]
      bd <- arbitrary[Option[BigDecimal]]
      f <- arbitrary[Option[Float]]
      d <- arbitrary[Option[Double]]
      s <- Gen.option(nonEmptyStringGen)
      b <- arbitrary[Option[Boolean]]
      ba <- arbitrary[Option[ByteBuffer]]
    } yield B1(i, l, bd, f, d, s, b, ba)

    def gen2: Gen[B2] = for {
      i <- arbitrary[List[Int]]
      l <- arbitrary[List[Long]]
      bd <- arbitrary[List[BigDecimal]]
      f <- arbitrary[List[Float]]
      d <- arbitrary[List[Double]]
      s <- Gen.listOf(nonEmptyStringGen)
      b <- arbitrary[List[Boolean]]
      ba <- arbitrary[List[ByteBuffer]]
    } yield B2(i, l, bd, f, d, s, b, ba)

    def gen3: Gen[B3] = for {
      a <- genB0
      b <- Gen.listOf(genB0)
      c <- Gen.option(genB0)
      d <- Gen.option(Gen.listOf(genB0))
      e <- Gen.listOf(Gen.option(genB0))
    } yield B3(a, b, c, d, e)
  }
}
