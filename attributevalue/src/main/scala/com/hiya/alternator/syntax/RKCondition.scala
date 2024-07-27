package com.hiya.alternator.syntax

import com.hiya.alternator.{ScalarDynamoFormat, StringLikeDynamoFormat}
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, QueryRequest}

import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters._

sealed abstract class RKCondition[+T] {
  def render(pkField: String, q: RKCondition.QueryBuilder): RKCondition.QueryBuilder
}

object RKCondition {
  case class QueryBuilder(
    exp: List[String],
    namesMap: Queue[String],
    valueMap: Queue[AttributeValue]
  ) {
    def newName(name: String)(f: String => QueryBuilder => QueryBuilder): QueryBuilder = {
      val nameP = s"#P${namesMap.size}"
      f(nameP)(copy(namesMap = namesMap.enqueue(name)))
    }

    def newParam(av: AttributeValue)(f: String => QueryBuilder => QueryBuilder): QueryBuilder = {
      val nameP = s":param${valueMap.size}"
      f(nameP)(copy(valueMap = valueMap.enqueue(av)))
    }

    def param(name: String, av: AttributeValue)(f: (String, String) => QueryBuilder => QueryBuilder): QueryBuilder = {
      newName(name)(paramName => _.newParam(av)(valueName => f(paramName, valueName)))
    }

    def add(exp: String): QueryBuilder = copy(exp = exp :: this.exp)

    def apply(q: QueryRequest.Builder): QueryRequest.Builder = {
      q.keyConditionExpression(exp.mkString(" AND "))
        .expressionAttributeNames(namesMap.zipWithIndex.map { case (name, idx) => s"#P${idx}" -> name }.toMap.asJava)
        .expressionAttributeValues(
          valueMap.zipWithIndex.map { case (name, idx) => s":param${idx}" -> name }.toMap.asJava
        )
    }
  }

  object QueryBuilder extends QueryBuilder(List.empty, Queue.empty, Queue.empty)

  val empty: RKCondition[Nothing] = new RKCondition[Nothing] {
    override def render(pkField: String, q: QueryBuilder): QueryBuilder = q
  }

  abstract class BinOp[T: ScalarDynamoFormat](op: String) extends RKCondition[T] {
    def rhs: T

    override def render(pkField: String, q: QueryBuilder): QueryBuilder = {
      q.param(pkField, ScalarDynamoFormat[T].write(rhs)) { case (field, value) =>
        _.add(s"$field $op $value")
      }
    }
  }

  case class EQ[T: ScalarDynamoFormat](override val rhs: T) extends BinOp("=")
  case class LE[T: ScalarDynamoFormat](rhs: T) extends BinOp("<=")
  case class LT[T: ScalarDynamoFormat](rhs: T) extends BinOp("<")
  case class GE[T: ScalarDynamoFormat](rhs: T) extends BinOp(">=")
  case class GT[T: ScalarDynamoFormat](rhs: T) extends BinOp(">")
  case class BEGINS_WITH[T: StringLikeDynamoFormat](rhs: T) extends RKCondition[T] {
    override def render(pkField: String, q: QueryBuilder): QueryBuilder = {
      q.param(pkField, ScalarDynamoFormat[T].write(rhs)) { case (field, value) =>
        _.add(s"begins_with($field, $value)")
      }
    }
  }

  case class BETWEEN[T: ScalarDynamoFormat](lower: T, upper: T) extends RKCondition[T] {
    override def render(pkField: String, q: QueryBuilder): QueryBuilder = {
      q.param(pkField, ScalarDynamoFormat[T].write(lower)) { case (field, lowerValue) =>
        _.newParam(ScalarDynamoFormat[T].write(upper)) { upperValue =>
          _.add(s"$field BETWEEN $lowerValue AND $upperValue")
        }
      }
    }
  }
}
