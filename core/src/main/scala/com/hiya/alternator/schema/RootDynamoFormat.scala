package com.hiya.alternator.schema

import com.hiya.alternator.schema.DynamoFormat.Result

import java.util
import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait RootDynamoFormat[T] extends DynamoFormat[T] {
  def readFields[AV: AttributeValue](av: java.util.Map[String, AV]): DynamoFormat.Result[T]
  def writeFields[AV: AttributeValue](value: T): java.util.Map[String, AV]

  override def read[AV](av: AV)(implicit AV: AttributeValue[AV]): DynamoFormat.Result[T] = {
    AV.map(av) match {
      case Some(m) => readFields(m)
      case None => Left(DynamoAttributeError.TypeError(av, "M"))
    }
  }

  override def write[AV](value: T)(implicit AV: AttributeValue[AV]): AV =
    AV.createMap(writeFields(value))

  override def isEmpty(value: T): Boolean = false

  override def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): RootDynamoFormat[B] =
    new RootDynamoFormat[B] {

      override def readFields[AV: AttributeValue](av: util.Map[String, AV]): Result[B] =
        RootDynamoFormat.this.readFields(av).fold(Left(_), f)

      override def writeFields[AV: AttributeValue](value: B): util.Map[String, AV] =
        RootDynamoFormat.this.writeFields(g(value))
    }
}

object RootDynamoFormat {
  def apply[T](implicit T: RootDynamoFormat[T]): RootDynamoFormat[T] = T

  trait Instances {
    implicit def mapDynamoFormat[T: DynamoFormat]: RootDynamoFormat[Map[String, T]] =
      new RootDynamoFormat[Map[String, T]] {
        def traverseMap[AV](
          m: mutable.Map[String, AV],
          f: AV => DynamoFormat.Result[T]
        ): DynamoFormat.Result[Map[String, T]] = {
          val b = Map.newBuilder[String, T]
          @tailrec
          def collectData(it: Iterator[(String, AV)]): DynamoFormat.Result[Map[String, T]] = {
            if (it.hasNext) {
              val (k, v) = it.next()
              f(v) match {
                case Right(data) =>
                  b += k -> data
                  collectData(it)
                case Left(error) =>
                  Left(error)
              }

            } else {
              Right(b.result())
            }
          }

          collectData(m.iterator)
        }

        override def readFields[AV: AttributeValue](
          av: java.util.Map[String, AV]
        ): DynamoFormat.Result[Map[String, T]] =
          traverseMap(av.asScala, DynamoFormat[T].read[AV])

        override def writeFields[AV: AttributeValue](value: Map[String, T]): java.util.Map[String, AV] =
          value.view.mapValues({ x => DynamoFormat[T].write(x) }).toMap.asJava
      }
  }
}
