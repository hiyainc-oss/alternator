package com.hiya.alternator

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import java.util
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait CompoundDynamoFormat[T] extends DynamoFormat[T] {
  def readFields(av: util.Map[String, AttributeValue]): DynamoFormat.Result[T]
  def writeFields(value: T): util.Map[String, AttributeValue]

  override def read(av: AttributeValue): DynamoFormat.Result[T] = {
    if (av.getM != null) readFields(av.getM)
    else Left(DynamoAttributeError.TypeError(av, "M"))
  }

  override def write(value: T): AttributeValue = {
    new AttributeValue().withM(writeFields(value))
  }

  override def isEmpty(value: T): Boolean = false

  override def emap[B](f: T => Either[DynamoAttributeError, B], g: B => T): CompoundDynamoFormat[B] = new CompoundDynamoFormat[B] {

    override def readFields(av: util.Map[String, AttributeValue]): DynamoFormat.Result[B] =
      CompoundDynamoFormat.this.readFields(av).fold(Left(_), f)

    override def writeFields(value: B): util.Map[String, AttributeValue] =
      CompoundDynamoFormat.this.writeFields(g(value))
  }
}

object CompoundDynamoFormat {
  def apply[T](implicit T: CompoundDynamoFormat[T]): CompoundDynamoFormat[T] = T

  trait Instances {
    implicit def mapDynamoFormat[T: DynamoFormat]: CompoundDynamoFormat[Map[String, T]] = new CompoundDynamoFormat[Map[String, T]] {
      def traverseMap(m: mutable.Map[String, AttributeValue], f: AttributeValue => DynamoFormat.Result[T]): DynamoFormat.Result[Map[String, T]] = {
        val b = Map.newBuilder[String, T]
        @tailrec
        def collectData(it: Iterator[(String, AttributeValue)]): DynamoFormat.Result[Map[String, T]] = {
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


      override def readFields(av: util.Map[String, AttributeValue]): DynamoFormat.Result[Map[String, T]] = {
        traverseMap(av.asScala, DynamoFormat[T].read)
      }

      override def writeFields(value: Map[String, T]): util.Map[String, AttributeValue] =
        value.view.mapValues({x => DynamoFormat[T].writeIfNotEmpty(x).getOrElse(DynamoFormat.NullAttributeValue)}).toMap.asJava
    }
  }
}
