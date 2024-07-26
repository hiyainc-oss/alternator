package com.hiya.alternator.syntax

import com.hiya.alternator.schema.{AttributeValue, ScalarDynamoFormat, StringLikeDynamoFormat}

import scala.collection.immutable.Queue

sealed abstract class RKCondition[+T] {
  def render[AV: AttributeValue](pkField: String, q: RKCondition.QueryBuilder[AV]): RKCondition.QueryBuilder[AV]
}

object RKCondition {
  case class QueryBuilder[AV: AttributeValue](
    exp: List[String],
    namesMap: Queue[String],
    valueMap: Queue[AV]
  ) {
    def newName(name: String)(f: String => QueryBuilder[AV] => QueryBuilder[AV]): QueryBuilder[AV] = {
      val nameP = s"#P${namesMap.size}"
      f(nameP)(copy(namesMap = namesMap.enqueue(name)))
    }

    def newParam(av: AV)(f: String => QueryBuilder[AV] => QueryBuilder[AV]): QueryBuilder[AV] = {
      val nameP = s":param${valueMap.size}"
      f(nameP)(copy(valueMap = valueMap.enqueue(av)))
    }

    def param(name: String, av: AV)(f: (String, String) => QueryBuilder[AV] => QueryBuilder[AV]): QueryBuilder[AV] = {
      newName(name)(paramName => _.newParam(av)(valueName => f(paramName, valueName)))
    }

    def add(exp: String): QueryBuilder[AV] = copy(exp = exp :: this.exp)

//    def apply(implicit q: QueryRequest.Builder[AV]): QueryRequest.Builder[AV] = {
//      q.keyConditionExpression(exp.mkString(" AND "))
//        .expressionAttributeNames(namesMap.zipWithIndex.map { case (name, idx) => s"#P${idx}" -> name}.toMap.asJava)
//        .expressionAttributeValues(valueMap.zipWithIndex.map { case (name, idx) => s":param${idx}" -> name}.toMap.asJava)
//    }
  }

//  object QueryBuilder extends QueryBuilder(List.empty, Queue.empty, Queue.empty)

  val empty: RKCondition[Nothing] = new RKCondition[Nothing] {
    override def render[AV: AttributeValue](pkField: String, q: QueryBuilder[AV]): QueryBuilder[AV] = q
  }

  private[syntax] abstract class BinOp[T: ScalarDynamoFormat](op: String) extends RKCondition[T] {
    def rhs: T

    override def render[AV: AttributeValue](pkField: String, q: QueryBuilder[AV]): QueryBuilder[AV] = {
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
    override def render[AV: AttributeValue](pkField: String, q: QueryBuilder[AV]): QueryBuilder[AV] = {
      q.param(pkField, ScalarDynamoFormat[T].write(rhs)) { case (field, value) =>
        _.add(s"begins_with($field, $value)")
      }
    }
  }

  case class BETWEEN[T: ScalarDynamoFormat](lower: T, upper: T) extends RKCondition[T] {
    override def render[AV: AttributeValue](pkField: String, q: QueryBuilder[AV]): QueryBuilder[AV] = {
      q.param(pkField, ScalarDynamoFormat[T].write(lower)) { case (field, lowerValue) =>
        _.newParam(ScalarDynamoFormat[T].write(upper)) { upperValue =>
          _.add(s"$field BETWEEN $lowerValue AND $upperValue")
        }
      }
    }
  }
}
