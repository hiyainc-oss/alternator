package com.hiya.alternator

import com.hiya.alternator.schema.AttributeValue

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

sealed trait TestAV
object TestAV {
  case class TAVMap(fields: Map[String, TestAV]) extends TestAV
  case class TAVString(s: String) extends TestAV
  case class TAVNumber(n: String) extends TestAV
  case class TAVBool(b: Boolean) extends TestAV
  case class TAVList(items: List[TestAV]) extends TestAV
  case class TAVStringSet(ss: Set[String]) extends TestAV
  case class TAVNumberSet(ns: Set[String]) extends TestAV
  case class TAVBinary(b: Array[Byte]) extends TestAV
  case class TAVBinarySet(bs: List[Array[Byte]]) extends TestAV
  case object TAVNull extends TestAV

  implicit val testAttributeValue: AttributeValue[TestAV] = new AttributeValue[TestAV] {
    def map(av: TestAV): Option[java.util.Map[String, TestAV]] = av match {
      case TAVMap(f) => Some(f.asJava)
      case _ => None
    }
    def createMap(m: java.util.Map[String, TestAV]): TestAV = TAVMap(m.asScala.toMap)

    def nullValue: TestAV = TAVNull
    def isNull(av: TestAV): Boolean = av == TAVNull

    def string(av: TestAV): Option[String] = av match { case TAVString(s) => Some(s); case _ => None }
    def createString(s: String): TestAV = TAVString(s)

    def bool(av: TestAV): Option[Boolean] = av match { case TAVBool(b) => Some(b); case _ => None }
    def trueValue: TestAV = TAVBool(true)
    def falseValue: TestAV = TAVBool(false)

    def list(av: TestAV): Option[java.util.List[TestAV]] = av match {
      case TAVList(xs) => Some(xs.asJava)
      case _ => None
    }
    def createList(av: java.util.List[TestAV]): TestAV = TAVList(av.asScala.toList)
    def emptyList: TestAV = TAVList(Nil)

    def stringSet(av: TestAV): Option[java.util.Collection[String]] = av match {
      case TAVStringSet(ss) => Some(ss.asJava)
      case _ => None
    }
    def createStringSet(v: java.util.Collection[String]): TestAV = TAVStringSet(v.asScala.toSet)

    def numberSet(av: TestAV): Option[java.util.Collection[String]] = av match {
      case TAVNumberSet(ns) => Some(ns.asJava)
      case _ => None
    }
    def createNumberSet(v: java.util.Collection[String]): TestAV = TAVNumberSet(v.asScala.toSet)

    def createBinary(v: Array[Byte]): TestAV = TAVBinary(v)
    def createBinary(v: ByteBuffer): TestAV = TAVBinary(v.array())
    def byteBuffer(av: TestAV): Option[ByteBuffer] = av match {
      case TAVBinary(b) => Some(ByteBuffer.wrap(b))
      case _ => None
    }
    def byteArray(av: TestAV): Option[Array[Byte]] = av match {
      case TAVBinary(b) => Some(b)
      case _ => None
    }

    def createByteBufferSet(v: Iterable[ByteBuffer]): TestAV = TAVBinarySet(v.map(_.array()).toList)
    def byteBufferSet(av: TestAV): Option[Iterable[ByteBuffer]] = av match {
      case TAVBinarySet(bs) => Some(bs.map(ByteBuffer.wrap))
      case _ => None
    }

    def numeric(av: TestAV): Option[String] = av match { case TAVNumber(n) => Some(n); case _ => None }
    def createNumeric(v: String): TestAV = TAVNumber(v)
  }
}
