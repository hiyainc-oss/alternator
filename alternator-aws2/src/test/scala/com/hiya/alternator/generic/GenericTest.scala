package com.hiya.alternator.generic

import com.hiya.alternator.aws2._
import com.hiya.alternator.schema.DynamoFormat
import org.scalacheck.Arbitrary._
import org.scalacheck._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.nio.ByteBuffer
import scala.reflect.runtime.universe._


class GenericTest extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import com.hiya.alternator.generic.GenericTest._
  import com.hiya.alternator.generic.auto._

  private def writeAndReread[A: DynamoFormat](gen: Gen[A], name: String): Unit = {
    it(s"should write and re-read attributevalue for $name") {
      forAll(gen) { a: A =>
        val attributeValue = DynamoFormat[A].write(a)
        withClue(attributeValue) {
          DynamoFormat[A].read(attributeValue) shouldBe Right(a)
        }
      }
    }
  }

  private def testReadWrite[A: DynamoFormat : TypeTag](gen: Gen[A], option: Boolean = true, list: Boolean = true): Unit = {
    val typeLabel = typeTag.tpe.toString

    testReadWrite0(gen, s"$typeLabel")
    if (option) testReadWrite0(Gen.option(gen), s"Option[$typeLabel]")
    if (list) testReadWrite0(Gen.listOf(gen), s"List[$typeLabel]")

    testReadWrite0(mapGen(gen), s"Map[String, $typeLabel]")
    if (option) testReadWrite0(mapGen(Gen.option(gen)), s"Map[String, Option[$typeLabel]]")
    if (list) testReadWrite0(mapGen(Gen.listOf(gen)), s"Map[String, List[$typeLabel]]")
  }

  private def testReadWrite0[A: DynamoFormat](gen: Gen[A], name: String): Unit = {
    writeAndReread(gen, s"$name")
    writeAndReread(gen.map(P(_)), s"P[$name]")
    writeAndReread(gen.map(x => Q(data = x)), s"Q[$name]")
    writeAndReread(gen.map(x => R(data = x)), s"R[$name]")
    writeAndReread(Gen.option[S[A]](gen.map(SB(_))).map(_.getOrElse(SA)), s"S[$name]")
  }


  private def withOption[A: DynamoFormat : TypeTag]()(implicit arb: Arbitrary[A]): Unit =
    testReadWrite(arb.arbitrary)

  private def withoutOption[A: DynamoFormat : TypeTag]()(implicit arb: Arbitrary[A]): Unit =
    testReadWrite(arb.arbitrary, option = false)

  private def withoutList[A: DynamoFormat : TypeTag]()(implicit arb: Arbitrary[A]): Unit =
    testReadWrite(arb.arbitrary, list = false)


  withOption[Byte]()
  withOption[Int]()
  withOption[Long]()
  withOption[Double]()
  withOption[Float]()
  withOption[BigDecimal]()
  withOption[String]()
  withOption[ByteBuffer]()
  withOption[Boolean]()
  withoutList[B]()
  withoutOption[Set[String]]()
  withoutOption[Set[Double]]()
  withoutOption[Set[Int]]()
//  withoutOption[Set[ByteBuffer]]()

}

object GenericTest {
  implicit val arbitraryByteBuffer: Arbitrary[ByteBuffer] = Arbitrary(arbitrary[Array[Byte]].map(ByteBuffer.wrap))

  val nonEmptyStringGen: Gen[String] =
    Gen.nonEmptyContainerOf[Array, Char](Arbitrary.arbChar.arbitrary).map(arr => new String(arr))

  def mapGen[T](gen: Gen[T]): Gen[Map[String, T]] =
    for {
      key <- nonEmptyStringGen
      value <- gen
      map <- Gen.mapOf(key -> value)
    } yield map


  case object CO

  sealed trait S[+A]
  final case object SA extends S[Nothing]
  final case class SB[A](data: A) extends S[A]

  final case class P[A](data: A)
  final case class Q[A](first: Int = 42, data: A)
  final case class R[A](data: A, last: Int = 42)

  sealed trait B
  final case class B0(i: Int, l: Long, bd: BigDecimal, f: Float, d: Double, s: String, b: Boolean, ba: ByteBuffer) extends B
  final case class B1(i: Option[Int], l: Option[Long], bd: Option[BigDecimal], f: Option[Float], d: Option[Double], s: Option[String], b: Option[Boolean], ba: Option[ByteBuffer]) extends B
  final case class B2(i: List[Int], l: List[Long], bd: List[BigDecimal], f: List[Float], d: List[Double], s: List[String], b: List[Boolean], ba: List[ByteBuffer]) extends B
  final case class B3(a: B, b: List[B], c: Option[B], d: Option[List[B]], e: List[Option[B]]) extends B


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
      s <- arbitrary[String]
      b <- arbitrary[Boolean]
      ba <- arbitrary[ByteBuffer]
    } yield B0(i, l, bd, f, d, s, b, ba)

    def gen1: Gen[B1] = for {
      i <- arbitrary[Option[Int]]
      l <- arbitrary[Option[Long]]
      bd <- arbitrary[Option[BigDecimal]]
      f <- arbitrary[Option[Float]]
      d <- arbitrary[Option[Double]]
      s <- arbitrary[Option[String]]
      b <- arbitrary[Option[Boolean]]
      ba <- arbitrary[Option[ByteBuffer]]
    } yield B1(i, l, bd, f, d, s, b, ba)

    def gen2: Gen[B2] = for {
      i <- arbitrary[List[Int]]
      l <- arbitrary[List[Long]]
      bd <- arbitrary[List[BigDecimal]]
      f <- arbitrary[List[Float]]
      d <- arbitrary[List[Double]]
      s <- arbitrary[List[String]]
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
