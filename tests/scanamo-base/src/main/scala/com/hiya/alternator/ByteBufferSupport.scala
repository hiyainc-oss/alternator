package com.hiya.alternator

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

import java.nio.ByteBuffer

object ByteBufferSupport {
  implicit val arbitraryByteBuffer: Arbitrary[ByteBuffer] = Arbitrary(arbitrary[Array[Byte]].map(ByteBuffer.wrap))

}
