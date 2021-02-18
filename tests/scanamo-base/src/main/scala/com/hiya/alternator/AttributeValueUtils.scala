package com.hiya.alternator

import com.amazonaws.services.dynamodbv2.{model => aws}
import org.scalactic.Equality
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.{model => aws2}

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._


object AttributeValueUtils {

  implicit class FromAWS2(av: aws2.AttributeValue) {
    def toAws: aws.AttributeValue = {
      if (av.nul() ne null)
        new aws.AttributeValue().withNULL(true)
      else if (av.b() ne null)
        new aws.AttributeValue().withB(ByteBuffer.wrap(av.b().asByteArray()))
      else if (av.bool() ne null)
        new aws.AttributeValue().withBOOL(av.bool())
      else if (av.hasBs)
        new aws.AttributeValue().withBS(av.bs().asScala.map(_.asByteBuffer()).asJava)
      else if (av.hasL)
        new aws.AttributeValue().withL(av.l().asScala.map(_.toAws).asJava)
      else if (av.hasM)
        new aws.AttributeValue().withM(av.m().asScala.view.mapValues(_.toAws).toMap.asJava)
      else if (av.n() ne null)
        new aws.AttributeValue().withN(av.n())
      else if (av.hasNs)
        new aws.AttributeValue().withNS(av.ns())
      else if (av.s() ne null)
        new aws.AttributeValue().withS(av.s())
      else if (av.hasSs)
        new aws.AttributeValue().withSS(av.ss())
      else
        throw new IllegalArgumentException("this should not happen")
    }
  }

  implicit class FromAWS(av: aws.AttributeValue) {

    def deepCopy(): aws.AttributeValue = {
      if (av.getNULL ne null)
        new aws.AttributeValue().withNULL(true)
      else if (av.getB ne null)
        new aws.AttributeValue().withB(ByteBuffer.wrap(av.getB.array()))
      else if (av.getBOOL ne null)
        new aws.AttributeValue().withBOOL(av.getBOOL)
      else if (av.getBS ne null)
        new aws.AttributeValue().withBS(av.getBS.asScala.map(x => ByteBuffer.wrap(x.array())).asJava)
      else if (av.getL ne null)
        new aws.AttributeValue().withL(av.getL.asScala.map(_.deepCopy()).asJava)
      else if (av.getM ne null)
        new aws.AttributeValue().withM(av.getM.asScala.view.mapValues(_.deepCopy()).toMap.asJava)
      else if (av.getN ne null)
        new aws.AttributeValue().withN(av.getN)
      else if (av.getNS ne null)
        new aws.AttributeValue().withNS(av.getNS)
      else if (av.getS ne null)
        new aws.AttributeValue().withS(av.getS)
      else if (av.getSS ne null)
        new aws.AttributeValue().withSS(av.getSS)
      else
        throw new IllegalArgumentException("this should not happen")
    }

    def toAws2: aws2.AttributeValue = {
      if (av.getNULL ne null)
        aws2.AttributeValue.builder().nul(true).build()
      else if (av.getB ne null)
        aws2.AttributeValue.builder().b(SdkBytes.fromByteBuffer(av.getB)).build()
      else if (av.getBOOL ne null)
        aws2.AttributeValue.builder().bool(av.getBOOL).build()
      else if (av.getBS ne null)
        aws2.AttributeValue
          .builder()
          .bs(av.getBS.asScala.map(SdkBytes.fromByteBuffer).asJava)
          .build()
      else if (av.getL ne null)
        aws2.AttributeValue.builder().l(av.getL.asScala.map(_.toAws2).asJava).build()
      else if (av.getM ne null)
        aws2.AttributeValue
          .builder()
          .m(av.getM.asScala.view.mapValues(_.toAws2).toMap.asJava)
          .build()
      else if (av.getN ne null)
        aws2.AttributeValue.builder().n(av.getN).build()
      else if (av.getNS ne null)
        aws2.AttributeValue.builder().ns(av.getNS).build()
      else if (av.getS ne null)
        aws2.AttributeValue.builder().s(av.getS).build()
      else if (av.getSS ne null)
        aws2.AttributeValue.builder().ss(av.getSS).build()
      else
        throw new IllegalArgumentException("this should not happen")
    }
  }

  implicit val awsEqAws2: Equality[aws2.AttributeValue] = new Equality[aws2.AttributeValue] {

    def areEqualSame(a: aws2.AttributeValue, b: aws2.AttributeValue): Boolean = {
      if (a.nul() ne null) {
        (b.nul() ne null) && a.nul() == b.nul()
      } else if (a.b() ne null)
        (b.b() ne null) && a.b() == b.b()
      else if (a.bool() ne null)
        (b.bool() ne null) && a.bool() == b.bool()
      else if (a.hasBs)
        (b.bs() ne null) && a.bs().asScala.toSet == b.bs().asScala.toSet
      else if (a.hasL)
        (b.l() ne null) && a.l().asScala.zip(b.l().asScala).forall(x => areEqualSame(x._1, x._2))
      else if (a.hasM)
        if (b.hasM) {
          val aa: mutable.Map[String, aws2.AttributeValue] = a.m().asScala
          val bb: mutable.Map[String, aws2.AttributeValue] = b.m().asScala
          val keys                                         = aa.keySet ++ bb.keySet
          keys.forall({ name =>
            (aa.get(name), bb.get(name)) match {
              case (Some(av), Some(bv)) => areEqual(av, bv)
              case _                    => false
            }
          })
        } else false
      else if (a.n() ne null)
        (b.n() ne null) && a.n() == b.n()
      else if (a.hasNs)
        (b.ns() ne null) && a.ns().asScala.toSet == b.ns().asScala.toSet
      else if (a.s() ne null)
        (b.s() ne null) && a.s() == b.s()
      else if (a.hasSs)
        (b.ss() ne null) && a.ss().asScala.toSet == b.ss().asScala.toSet
      else
        false
    }

    override def areEqual(a: aws2.AttributeValue, b: Any): Boolean = b match {
      case b: aws2.AttributeValue => areEqualSame(a, b)
      case b: aws.AttributeValue  => areEqualSame(a, b.toAws2)
      case _                      => false
    }
  }
}
