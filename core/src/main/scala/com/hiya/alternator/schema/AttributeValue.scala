package com.hiya.alternator.schema

import java.nio.ByteBuffer

trait AttributeValue[AV] {
  def map(av: AV): Option[java.util.Map[String, AV]]
  def createMap(map: java.util.Map[String, AV]): AV

  def nullValue: AV
  def isNull(av: AV): Boolean

  def string(av: AV): Option[String]
  def createString(s: String): AV

  def bool(av: AV): Option[Boolean]
  def trueValue: AV
  def falseValue: AV

  def list(av: AV): Option[java.util.List[AV]]
  def createList(av: java.util.List[AV]): AV
  def emptyList: AV

  def stringSet(av: AV): Option[java.util.Collection[String]]
  def createStringSet(value: java.util.Collection[String]): AV

  def createNumberSet(value: java.util.Collection[String]): AV
  def numberSet(av: AV): Option[java.util.Collection[String]]

  def createBinary(value: Array[Byte]): AV
  def createBinary(value: ByteBuffer): AV
  def byteBuffer(av: AV): Option[ByteBuffer]
  def byteArray(av: AV): Option[Array[Byte]]

  def createByteBufferSet(value: Iterable[ByteBuffer]): AV
  def byteBufferSet(av: AV): Option[Iterable[ByteBuffer]]

  def numeric(av: AV): Option[String]
  def createNumeric(value: String): AV
}
