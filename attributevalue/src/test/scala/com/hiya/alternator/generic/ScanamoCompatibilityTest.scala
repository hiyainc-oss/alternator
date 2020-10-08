package com.hiya.alternator.generic

import java.nio.ByteBuffer

import akka.util.ByteString
import com.hiya.alternator.AttributeValueUtils._
import com.hiya.alternator.DynamoFormat
import org.scalacheck.Arbitrary._
import org.scalacheck._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scanamo.generic.auto._
import org.scanamo.{DynamoFormat => ScanamoFormat}

import scala.reflect.runtime.universe._


class ScanamoCompatibilityTest extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import ScanamoCompatibilityTest._
  import auto._

  private def testReadWrite1[A: DynamoFormat: TypeTag: ScanamoFormat](gen: Gen[A]): Unit = {
    val typeLabel = typeTag[A].tpe.toString
    it(s"should generate the same as scanamo for $typeLabel") {
      forAll(gen) { a: A =>
        val aws = ScanamoFormat[A].write(a).toAttributeValue
        val aws2 = DynamoFormat[A].write(a)
        val aws2from1 = aws.toAws2
        val aws1from2 = aws2.toAws

        aws2 shouldEqual aws2from1
        DynamoFormat[A].read(aws2) shouldBe ScanamoFormat[A].read(aws)
        DynamoFormat[A].read(aws2from1) shouldBe ScanamoFormat[A].read(aws1from2)
      }
    }
  }

  private def testReadWrite[A: DynamoFormat: TypeTag: ScanamoFormat](gen: Gen[A]): Unit = {
    testReadWrite1(gen)
    testReadWrite1(gen.map(P(_)))

    val x: Gen[S[A]] = Gen.oneOf(gen.map(SB(_)), Gen.const(SA))
    testReadWrite1(x)
  }


  private def testReadWrite[A: DynamoFormat: TypeTag: ScanamoFormat]()(implicit arb: Arbitrary[A]): Unit =
    testReadWrite(arb.arbitrary)


  testReadWrite[Int]()
  testReadWrite[Set[Long]]()
  // Generate limited values for double and big decimal
  // see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes.Number
  testReadWrite[Set[Double]](Gen.containerOf[Set, Double](Arbitrary.arbLong.arbitrary.map(_.toDouble)))
  testReadWrite[Set[BigDecimal]](
    Gen.containerOf[Set, BigDecimal](Arbitrary.arbLong.arbitrary.map(BigDecimal(_)))
  )

  testReadWrite[Set[String]](Gen.containerOf[Set, String](nonEmptyStringGen))

  testReadWrite[Option[String]](Gen.option(nonEmptyStringGen))
  testReadWrite[Option[Int]]()
  testReadWrite[Map[String, Long]](Gen.mapOf[String, Long] {
    for {
      key <- nonEmptyStringGen
      value <- Arbitrary.arbitrary[Long]
    } yield key -> value
  })
  testReadWrite[List[String]](Gen.listOf(nonEmptyStringGen))
  testReadWrite[List[Int]](Gen.listOfN(0, Gen.posNum[Int]))

  implicit val byteStringFormat: ScanamoFormat[ByteString] = ScanamoFormat.iso[ByteString, ByteBuffer](ByteString(_), _.toByteBuffer)

  testReadWrite[ByteString]()

  testReadWrite[B]()
//  testReadWrite[CO.type](Gen.const(CO))
}

object ScanamoCompatibilityTest {

  case object CO

  sealed trait S[+A]
  final case object SA extends S[Nothing]
  final case class SB[A](data: A) extends S[A]

  final case class P[A](data: A)

  sealed trait B
  final case class B0(i: Int, l: Long, bd: BigDecimal, f: Float, d: Double, s: String, b: Boolean, ba: ByteString) extends B
  final case class B1(i: Option[Int], l: Option[Long], bd: Option[BigDecimal], f: Option[Float], d: Option[Double], s: Option[String], b: Option[Boolean], ba: Option[ByteString]) extends B
  final case class B2(i: List[Int], l: List[Long], bd: List[BigDecimal], f: List[Float], d: List[Double], s: List[String], b: List[Boolean], ba: List[ByteString]) extends B
  final case class B3(a: B, b: List[B], c: Option[B], d: Option[List[B]], e: List[Option[B]]) extends B

  private val nonEmptyStringGen: Gen[String] =
    Gen.nonEmptyContainerOf[Array, Char](Arbitrary.arbChar.arbitrary).map(arr => new String(arr))

  implicit val arbByteString: Arbitrary[ByteString] = Arbitrary(arbitrary[Array[Byte]].map(ByteString(_)))

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
      ba <- arbitrary[ByteString]
    } yield B0(i, l, bd, f, d, s, b, ba)

    def gen1: Gen[B1] = for {
      i <- arbitrary[Option[Int]]
      l <- arbitrary[Option[Long]]
      bd <- arbitrary[Option[BigDecimal]]
      f <- arbitrary[Option[Float]]
      d <- arbitrary[Option[Double]]
      s <- Gen.option(nonEmptyStringGen)
      b <- arbitrary[Option[Boolean]]
      ba <- arbitrary[Option[ByteString]]
    } yield B1(i, l, bd, f, d, s, b, ba)

    def gen2: Gen[B2] = for {
      i <- arbitrary[List[Int]]
      l <- arbitrary[List[Long]]
      bd <- arbitrary[List[BigDecimal]]
      f <- arbitrary[List[Float]]
      d <- arbitrary[List[Double]]
      s <- Gen.listOf(nonEmptyStringGen)
      b <- arbitrary[List[Boolean]]
      ba <- arbitrary[List[ByteString]]
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
