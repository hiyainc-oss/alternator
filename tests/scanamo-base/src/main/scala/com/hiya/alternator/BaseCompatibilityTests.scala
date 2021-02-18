package com.hiya.alternator

import com.hiya.alternator.generic.auto._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

trait BaseCompatibilityTests extends ScalaCheckDrivenPropertyChecks with AnyFunSpecLike {
  import BaseCompatibilityTests._
  import ByteBufferSupport._

  trait ScanamoFormatBase[T] {
    def compare(a: T, aws2: AttributeValue, read: AttributeValue => DynamoFormat.Result[T]): Unit
  }

  type ScanamoFormat[T] <: ScanamoFormatBase[T]

  protected def fieldTest[T : ScanamoFormat]: ScanamoFormat[Field[T]]
  protected def optionTest[T : ScanamoFormat]: ScanamoFormat[Option[T]]
  protected def listTest[T : ScanamoFormat]: ScanamoFormat[List[T]]
  protected def mapTest[T : ScanamoFormat]: ScanamoFormat[Map[String, T]]

  private def testReadWrite[A: DynamoFormat : TypeTag](gen: Gen[A])(implicit A: ScanamoFormat[A]): Unit = {
    val typeLabel = typeTag[A].tpe.toString

    implicit def fieldTestT[T : ScanamoFormat]: ScanamoFormat[Field[T]] =  fieldTest
    implicit def optionTestT[T : ScanamoFormat]: ScanamoFormat[Option[T]] = optionTest
    implicit def listTestT[T : ScanamoFormat]: ScanamoFormat[List[T]] = listTest
    implicit def mapTestT[T : ScanamoFormat]: ScanamoFormat[Map[String, T]] = mapTest

    def test[T: DynamoFormat](gen: Gen[T], name: String)(implicit T: ScanamoFormat[T]): Unit = {

      it(s"should generate the same as scanamo for $name") {
        forAll(gen) { a: T =>
          val aws2 = DynamoFormat[T].write(a)
          T.compare(a, aws2, DynamoFormat[T].read)
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

  def scanamo()(
    implicit
    byteFormat: ScanamoFormat[Byte],
    intFormat: ScanamoFormat[Int],
    longFormat: ScanamoFormat[Long],
    doubleFormat: ScanamoFormat[Double],
    floatFormat: ScanamoFormat[Float],
    bigdecimalFormat: ScanamoFormat[BigDecimal],
    stringFormat: ScanamoFormat[String],
    byteStringFormat: ScanamoFormat[ByteBuffer],
    booleanFormat: ScanamoFormat[Boolean],
    stringSetFormat: ScanamoFormat[Set[String]],
    intSetFormat: ScanamoFormat[Set[Int]]
  ): Unit = {
    testReadWrite[Byte]()
    testReadWrite[Int]()
    testReadWrite[Long]()
    testReadWrite[Double]()
    testReadWrite[Float]()
    testReadWrite[BigDecimal]()
    testReadWrite[String]()
    testReadWrite[ByteBuffer]()
    testReadWrite[Boolean]()
    testReadWrite[Set[String]]()
    testReadWrite[Set[Int]]()
//    testReadWrite[Set[Array[Byte]]]() // Incompatible, scanamo throws an exception
  }
}

object BaseCompatibilityTests {

  final case class Field[T](value: T)
}

