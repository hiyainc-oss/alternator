package com.hiya.alternator

import java.nio.ByteBuffer

import akka.util.ByteString
import com.hiya.alternator.AttributeValueUtils._
import com.hiya.alternator.generic.auto._
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

  private def testReadWrite[A: DynamoFormat : TypeTag : ScanamoFormat](gen: Gen[A]): Unit = {
    val typeLabel = typeTag[A].tpe.toString

    def test[T: DynamoFormat : ScanamoFormat](gen: Gen[T], name: String): Unit = {

      it(s"should generate the same as scanamo for $name") {
        forAll(gen) { a: T =>
          val aws = ScanamoFormat[T].write(a).toAttributeValue
          val aws2 = DynamoFormat[T].write(a)

          aws2 shouldEqual aws.toAws2
          DynamoFormat[T].read(aws2).shouldEqual(ScanamoFormat[T].read(aws.deepCopy()))
          DynamoFormat[T].read(aws.toAws2).shouldEqual(ScanamoFormat[T].read(aws2.toAws))
        }
      }
    }

    test(gen.map(Field(_)), s"$typeLabel")
    test(Gen.option(gen).map(Field(_)), s"Option[$typeLabel]")
    test(Gen.listOf(gen).map(Field(_)), s"List[$typeLabel]")

    val nonEmptyStringGen: Gen[String] =
      Gen.nonEmptyContainerOf[Array, Char](Arbitrary.arbChar.arbitrary).map(arr => new String(arr))

    def mapGen[T](gen: Gen[T]) =
      for {
        key <- nonEmptyStringGen
        value <- gen
        map <- Gen.mapOf(key -> value)
      } yield map


    test(mapGen(gen).map(Field(_)), s"Map[String, $typeLabel]")
    test(mapGen(Gen.option(gen)).map(Field(_)), s"Map[String, Option[$typeLabel]]")
    test(mapGen(Gen.listOf(gen)).map(Field(_)), s"Map[String, List[$typeLabel]]")
  }


  private def testReadWrite[A: DynamoFormat : TypeTag : ScanamoFormat]()(implicit arb: Arbitrary[A]): Unit =
    testReadWrite(arb.arbitrary)

  testReadWrite[Byte]()
  testReadWrite[Int]()
  testReadWrite[Long]()
  testReadWrite[Double]()
  testReadWrite[Float]()
  testReadWrite[BigDecimal]()
  testReadWrite[String]()
  testReadWrite[ByteString]()

  testReadWrite[Boolean]()
  testReadWrite[Set[String]]()
  testReadWrite[Set[Int]]()
  //  testReadWrite[Set[Array[Byte]]]() // Incompatible
  testReadWrite[Map[String, Int]]()
}

object ScanamoCompatibilityTest {
  implicit val byteStringFormat: ScanamoFormat[ByteString] = ScanamoFormat.iso[ByteString, ByteBuffer](ByteString(_), _.toByteBuffer)
  implicit val arbitraryByteString: Arbitrary[ByteString] = Arbitrary(arbitrary[Array[Byte]].map(ByteString.fromArray))

  final case class Field[T](value: T)
}
