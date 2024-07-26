package com.hiya.alternator.schema

import scala.util.Try

trait NumericCompat {

  trait Numeric[T] {
    def parseString(str: String): Option[T]
  }

  object Numeric {
    def apply[T: Numeric]: Numeric[T] = implicitly[Numeric[T]]

    implicit object LongIsNumeric extends Numeric[Long] {
      override def parseString(str: String): Option[Long] = Try(str.toLong).toOption
    }

    implicit object IntIsNumeric extends Numeric[Int] {
      override def parseString(str: String): Option[Int] = Try(str.toInt).toOption
    }

    implicit object ShortIsNumeric extends Numeric[Short] {
      override def parseString(str: String): Option[Short] = Try(str.toShort).toOption
    }

    implicit object ByteIsNumeric extends Numeric[Byte] {
      override def parseString(str: String): Option[Byte] = Try(str.toByte).toOption
    }

    implicit object CharIsNumeric extends Numeric[Char] {
      override def parseString(str: String): Option[Char] = Try(str.toInt.toChar).toOption
    }

    implicit object BigDecimalIsNumeric extends Numeric[BigDecimal] {
      override def parseString(str: String): Option[BigDecimal] = Try(BigDecimal(str)).toOption
    }

    implicit object BigIntIsNumeric extends Numeric[BigInt] {
      override def parseString(str: String): Option[BigInt] = Try(BigInt(str)).toOption
    }

    implicit object FloatIsNumeric extends Numeric[Float] {
      override def parseString(str: String): Option[Float] = Try(str.toFloat).toOption
    }

    implicit object DoubleIsNumeric extends Numeric[Double] {
      override def parseString(str: String): Option[Double] = Try(str.toDouble).toOption
    }
  }
}
