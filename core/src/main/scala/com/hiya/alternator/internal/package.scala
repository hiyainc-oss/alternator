package com.hiya.alternator

import cats.FlatMap
import cats.data.State
import cats.syntax.all._
import com.hiya.alternator.schema.{AttributeValue, TableSchemaWithRange}
import com.hiya.alternator.syntax.ConditionExpression.{ArrayIndex, Attr, BinOp, FunCall, Literal, MapIndex}
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition}

package object internal {
  type Condition[AV, T] = State[ConditionParameters[AV], T]

  object Condition {
    def eval[AV, T](params: Condition[AV, T]): T = params.runA(ConditionParameters.empty[AV]).value
    def pure[AV, T](value: T): Condition[AV, T] = State.pure(value)
    def execute[AV, Builder](builder: Builder)(implicit
      Builder: ConditionalSupport[Builder, AV]
    ): Condition[AV, Builder] =
      State { params => (params, params(builder)) }

    def getParam[AV](name: String): Condition[AV, String] =
      State { params =>
        val idx = params.attributeNames.getOrElse(name, s"#a${params.attributeNames.size}")
        (params.copy(attributeNames = params.attributeNames.updated(name, idx)), idx)
      }

    def getValue[AV](value: AV): Condition[AV, String] =
      State { params =>
        val idx = params.attributeValues.getOrElse(value, s":v${params.attributeValues.size}")
        (params.copy(attributeValues = params.attributeValues.updated(value, idx)), idx)
      }

    def renderCondition[AV: AttributeValue](expression: ConditionExpression[_]): Condition[AV, String] =
      expression match {
        case Attr(name) =>
          getParam(name)

        case ArrayIndex(base, index) =>
          for {
            expr <- renderCondition[AV](base)
          } yield s"$expr[$index]"

        case MapIndex(base, fieldName) =>
          for {
            expr <- renderCondition[AV](base)
            fieldName <- getParam[AV](fieldName)
          } yield s"$expr.$fieldName"

        case l @ Literal(_) =>
          getValue(l.write[AV])

        case FunCall(name, args) =>
          for {
            args <- args.traverse(renderCondition[AV])
          } yield s"$name(${args.mkString(",")})"

        case BinOp(op, lhs, rhs) =>
          for {
            lhs <- renderCondition[AV](lhs)
            rhs <- renderCondition[AV](rhs)
          } yield s"($lhs) $op ($rhs)"
      }

    private def nonemptyCondition[AV: AttributeValue](
      exp: RKCondition.NonEmpty[_],
      fieldName: String
    ): Condition[AV, String] = {
      for {
        field <- getParam(fieldName)
        ret <- exp match {
          case op: RKCondition.BinOp[_] =>
            getValue(op.av).map { value => s"$field ${op.op} $value" }
          case op: RKCondition.BEGINS_WITH[_] =>
            getValue(op.av).map { value => s"begins_with($field, $value)" }
          case op: RKCondition.BETWEEN[_] =>
            for {
              lowerValue <- getValue(op.lowerAv)
              upperValue <- getValue(op.upperAv)
            } yield s"$field BETWEEN $lowerValue AND $upperValue"
        }
      } yield ret
    }

    private def rkCondition[AV: AttributeValue](
      exp: RKCondition[_],
      fieldName: String
    ): Condition[AV, Option[String]] = {
      exp match {
        case RKCondition.Empty =>
          Condition.pure(None)
        case op: RKCondition.NonEmpty[_] =>
          nonemptyCondition(op, fieldName).map(_.some)
      }
    }

    def renderCondition[AV: AttributeValue, PK, RK](
      pk: PK,
      expression: RKCondition[RK],
      schema: TableSchemaWithRange.Aux[_, PK, RK]
    ): Condition[AV, String] = {
      for {
        pkCondition <- nonemptyCondition(RKCondition.EQ(pk)(schema.PK), schema.pkField)
        rkCondition <- rkCondition(expression, schema.rkField)
      } yield rkCondition match {
        case Some(rk) => s"($pkCondition) AND ($rk)"
        case None => pkCondition
      }
    }

  }

  implicit class OptApp[T](underlying: T) {

    def optApp[A](f: T => A => T): Option[A] => T = {
      case Some(a) => f(underlying)(a)
      case None => underlying
    }
  }

  implicit class OptAppF[F[_]: FlatMap, T](underlying: F[T]) {

    def optApp[A](f: T => A => T): Option[A] => F[T] = {
      case Some(a) => underlying.map(f(_)(a))
      case None => underlying
    }

    def optAppF[A](f: T => A => T): Option[F[A]] => F[T] = {
      case Some(a) =>
        for {
          t <- underlying
          p <- a
        } yield f(t)(p)

      case None => underlying
    }

  }
}
