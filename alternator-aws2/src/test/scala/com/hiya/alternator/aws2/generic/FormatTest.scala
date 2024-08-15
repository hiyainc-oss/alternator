package com.hiya.alternator.aws2.generic

import com.hiya.alternator.aws2._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.DynamoAttributeError.AttributeIsNull
import com.hiya.alternator.schema.{CompoundDynamoFormat, DynamoFormat}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._


class FormatTest extends AnyFunSpec with Matchers {
  private def iso[T](orig: T)(implicit T: DynamoFormat[T]): Assertion = {
    T.read(T.write[AttributeValue](orig)) shouldBe Right(orig)
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
      final case class Test(a: Int)
      iso(Test(1))(semiauto.deriveCompound[Test])
    }

    it("should derive a sealed trait") {
      sealed abstract class A
      final case class B(s: String) extends A
      final case object C extends A

      implicit val b: CompoundDynamoFormat[B] = semiauto.deriveCompound[B]
      implicit val a: CompoundDynamoFormat[A] = semiauto.deriveCompound[A]

      iso[A](B("asd"))
      iso[A](C)
    }
  }

  describe("basic formats") {
    val nullAv = AttributeValue.builder().nul(true).build()

    sealed trait NullValue[+T]
    case object Error extends NullValue[Nothing]
    case class Refl[T](value: T) extends NullValue[T]
    case class Irrefl[T](value: T) extends NullValue[T]

    def dynamo[T : TypeTag](nullValue: NullValue[T], tests: (T, AttributeValue)*)(implicit T : DynamoFormat[T]): Unit = {
      it(s"should read and write ${typeTag[T].tpe}") {
        nullValue match {
          case Irrefl(value) =>
            T.read(nullAv) shouldBe Right(value)
          case Refl(value) =>
            T.read(nullAv) shouldBe Right(value)
            T.write(value) shouldBe nullAv
          case Error =>
            T.read(nullAv) shouldBe Left(AttributeIsNull)
        }

        tests.foreach { case (value, av) =>
          T.read(av) shouldBe Right(value)
          T.write(value) shouldBe av
        }
      }
    }

    it should behave like dynamo[String](
      nullValue = Irrefl(""),
      "asd" -> AttributeValue.builder().s("asd").build()
    )

    it should behave like dynamo[Option[String]](
      nullValue = Refl(None),
      Some("asd") -> AttributeValue.builder().s("asd").build()
    )

    it should behave like dynamo[List[String]](
      nullValue = Error,
      Nil -> AttributeValue.builder().l(List.empty[AttributeValue].asJava).build(),
      List("asd") -> AttributeValue.builder().l(List(AttributeValue.builder().s("asd").build()).asJava).build()
    )

    it should behave like dynamo[Option[List[String]]](
      nullValue = Refl(None),
      Some(Nil) -> AttributeValue.builder().l(List.empty[AttributeValue].asJava).build(),
      Some(List("asd")) -> AttributeValue.builder().l(List(AttributeValue.builder().s("asd").build()).asJava).build()
    )

    it should behave like dynamo[List[Option[String]]](
      nullValue = Error,
      Nil -> AttributeValue.builder().l(List.empty[AttributeValue].asJava).build(),
      List(None) -> AttributeValue.builder().l(List(AttributeValue.builder().nul(true).build()).asJava).build(),
      List(Some("asd")) -> AttributeValue.builder().l(List(AttributeValue.builder().s("asd").build()).asJava).build()
    )

    it should behave like dynamo[Set[String]](
      nullValue = Refl(Set.empty),
      Set("asd") -> AttributeValue.builder().ss("asd").build(),
    )

    it should behave like dynamo[Int](
      nullValue = Error,
      42 -> AttributeValue.builder().n("42").build(),
    )

    it should behave like dynamo[Double](
      nullValue = Error,
      137.04 -> AttributeValue.builder().n("137.04").build(), // ℏc/e²
    )

  }
}
