package com.hiya.alternator.aws2

import akka.util.ByteString
import com.hiya.alternator.AttributeValueUtils._
import com.hiya.alternator.DynamoFormat
import com.hiya.alternator.generic.{CompatibilityTests, auto}
import org.scalacheck.Arbitrary._
import org.scalacheck._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scanamo.{DynamoFormat => ScanamoFormat}

import java.nio.ByteBuffer
import scala.reflect.runtime.universe._


class GenericTest extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import CompatibilityTests._
  import auto._
  import org.scanamo.generic.auto._

  private def testReadWrite1[A: DynamoFormat : TypeTag : ScanamoFormat](gen: Gen[A]): Unit = {
    val typeLabel = typeTag[A].tpe.toString
    it(s"should generate the same as scanamo for $typeLabel") {
      forAll(gen) { a: A =>
        val aws = ScanamoFormat[A].write(a).toAttributeValue
        val aws2 = DynamoFormat[A].write(a)

        aws2 shouldEqual aws
        DynamoFormat[A].read(aws2) shouldBe ScanamoFormat[A].read(aws)
        DynamoFormat[A].read(aws) shouldBe ScanamoFormat[A].read(aws2)
      }
    }
  }

  private def testReadWrite[A: DynamoFormat : TypeTag : ScanamoFormat](gen: Gen[A]): Unit = {
    testReadWrite1(gen)
    testReadWrite1(gen.map(P(_)))

    val x: Gen[S[A]] = Gen.oneOf(gen.map(SB(_)), Gen.const(SA))
    testReadWrite1(x)
  }


  private def testReadWrite[A: DynamoFormat : TypeTag : ScanamoFormat]()(implicit arb: Arbitrary[A]): Unit =
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
