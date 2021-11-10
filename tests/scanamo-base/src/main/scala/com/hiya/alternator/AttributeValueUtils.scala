package com.hiya.alternator

import com.amazonaws.services.dynamodbv2.{model => aws}
import org.scalactic.Equality

import scala.collection.mutable
import scala.jdk.CollectionConverters._


object AttributeValueUtils {
  implicit val awsEqAws2: Equality[aws.AttributeValue] = new Equality[aws.AttributeValue] {

    def areEqualSame(a: aws.AttributeValue, b: aws.AttributeValue): Boolean = {
      if (a.getNULL) {
        b.getNULL
      } else if (a.getB ne null)
        (b.getB ne null) && a.getB == b.getB
      else if (a.getBOOL ne null)
        (b.getBOOL ne null) && a.getBOOL == b.getBOOL
      else if (a.getBS ne null)
        (b.getBS ne null) && a.getBS.asScala.toSet == b.getBS.asScala.toSet
      else if (a.getL ne null)
        (b.getL ne null) && a.getL.asScala.zip(b.getL.asScala).forall(x => areEqualSame(x._1, x._2))
      else if (a.getM ne null)
        if (b.getM ne null) {
          val aa: mutable.Map[String, aws.AttributeValue] = a.getM.asScala
          val bb: mutable.Map[String, aws.AttributeValue] = b.getM.asScala
          val keys                                         = aa.keySet ++ bb.keySet
          keys.forall({ name =>
            (aa.get(name), bb.get(name)) match {
              case (Some(av), Some(bv)) => areEqual(av, bv)
              case _                    => false
            }
          })
        } else false
      else if (a.getN ne null)
        (b.getN ne null) && a.getN == b.getN
      else if (a.getNS ne null)
        (b.getNS ne null) && a.getNS.asScala.toSet == b.getNS.asScala.toSet
      else if (a.getS ne null)
        (b.getS ne null) && a.getS == b.getS
      else if (a.getSS ne null)
        (b.getSS ne null) && a.getSS.asScala.toSet == b.getSS.asScala.toSet
      else
        false
    }

    override def areEqual(a: aws.AttributeValue, b: Any): Boolean = b match {
      case b: aws.AttributeValue => areEqualSame(a, b)
      case _                     => false
    }
  }
}
