package com.hiya.alternator.aws1

import com.hiya.alternator.BaseCompatibilityTests.Field
import com.hiya.alternator.DynamoFormat.Result
import com.hiya.alternator.{AttributeValueUtils, BaseCompatibilityTests}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scanamo.DynamoFormat
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class CompatibilityTests  extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks with BaseCompatibilityTests {
  import AttributeValueUtils._

  class MyScanamoFormat[T](implicit val dynamoFormat : DynamoFormat[T]) extends ScanamoFormatBase[T] {
    override def compare(a: T, aws2: AttributeValue, read: AttributeValue => Result[T]): Unit = {
      val aws = DynamoFormat[T].write(a).toAttributeValue

      aws2 shouldEqual aws.toAws2
      read(aws2) shouldEqual DynamoFormat[T].read(aws.deepCopy())
      read(aws.toAws2) shouldEqual DynamoFormat[T].read(aws2.toAws)

      ()
    }
  }

  object MyScanamoFormat {
    def apply[T](implicit T: MyScanamoFormat[T]): MyScanamoFormat[T] = T
  }
  override type ScanamoFormat[T] = MyScanamoFormat[T]


  implicit def deriveScanamoFormat[T : DynamoFormat]: MyScanamoFormat[T] = new MyScanamoFormat[T]

  override protected def fieldTest[T](implicit f: MyScanamoFormat[T]): MyScanamoFormat[Field[T]] = {
    import f.dynamoFormat
    import org.scanamo.generic.auto._
    new MyScanamoFormat[Field[T]]()
  }

  override protected def optionTest[T](implicit f: MyScanamoFormat[T]): MyScanamoFormat[Option[T]] = {
    import f.dynamoFormat
    new MyScanamoFormat[Option[T]]()
  }

  override protected def listTest[T](implicit f: MyScanamoFormat[T]): MyScanamoFormat[List[T]] = {
    import f.dynamoFormat
    new MyScanamoFormat[List[T]]()
  }

  override protected def mapTest[T](implicit f: MyScanamoFormat[T]): MyScanamoFormat[Map[String, T]] = {
    import f.dynamoFormat
    new MyScanamoFormat[Map[String, T]]()
  }


  it should behave like scanamo()

}

