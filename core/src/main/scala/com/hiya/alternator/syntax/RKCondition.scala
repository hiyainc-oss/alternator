package com.hiya.alternator.syntax

import com.hiya.alternator.schema.{AttributeValue, ScalarDynamoFormat, StringLikeDynamoFormat}

sealed trait RKCondition[+T]

object RKCondition {
  sealed trait NonEmpty[+T] extends RKCondition[T]

  case object Empty extends RKCondition[Nothing]

  private[alternator] abstract class BinOp[T: ScalarDynamoFormat](val op: String) extends NonEmpty[T] {
    def rhs: T
    def av[AV: AttributeValue]: AV = ScalarDynamoFormat[T].write(rhs)
  }

  case class EQ[T: ScalarDynamoFormat](rhs: T) extends BinOp("=")
  case class LE[T: ScalarDynamoFormat](rhs: T) extends BinOp("<=")
  case class LT[T: ScalarDynamoFormat](rhs: T) extends BinOp("<")
  case class GE[T: ScalarDynamoFormat](rhs: T) extends BinOp(">=")
  case class GT[T: ScalarDynamoFormat](rhs: T) extends BinOp(">")

  case class BEGINS_WITH[T: StringLikeDynamoFormat](rhs: T) extends NonEmpty[T] {
    def av[AV: AttributeValue]: AV = ScalarDynamoFormat[T].write(rhs)
  }

  case class BETWEEN[T: ScalarDynamoFormat](lower: T, upper: T) extends NonEmpty[T] {
    def lowerAv[AV: AttributeValue]: AV = ScalarDynamoFormat[T].write(lower)
    def upperAv[AV: AttributeValue]: AV = ScalarDynamoFormat[T].write(upper)
  }
}
