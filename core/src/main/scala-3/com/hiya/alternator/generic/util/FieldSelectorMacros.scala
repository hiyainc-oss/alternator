package com.hiya.alternator.generic.util

import com.hiya.alternator.syntax.ConditionExpression

import scala.quoted.*

object FieldSelectorMacros {

  def field[V: Type, F: Type](selector: Expr[V => F])(using Quotes): Expr[ConditionExpression.Path[V, F]] = {
    import quotes.reflect.*

    // Each Term is already fully typechecked by the time the macro runs (selector's static
    // type is V => F), so `term.tpe` gives the real, precise type of that field — no need to
    // fall back to Any and cast at the end.
    def segments(term: Term): List[(String, TypeRepr)] = term match {
      case Select(inner, name) => segments(inner) :+ (name, term.tpe.widen)
      case Ident(_) => Nil
      case _ =>
        report.errorAndAbort(
          s"field(...) requires a plain field-accessor chain, e.g. field[${Type.show[V]}](_.a.b). Got: ${term.show}"
        )
    }

    def body(term: Term): Term = term match {
      case Inlined(_, _, inner) => body(inner)
      case Block(List(DefDef(_, _, _, Some(b))), _) => b
      case Lambda(_, b) => b
      case _ =>
        report.errorAndAbort(s"field(...) requires a lambda literal, e.g. field[${Type.show[V]}](_.a.b).")
    }

    // No explicit field-existence check needed: `selector` only reaches this point already
    // typechecked as V => F, so every segment (head and nested) is already a real, accessible
    // member of its receiver — the ordinary Dotty typer rejects `_.nonexistent` before macro
    // expansion even begins.
    val path = segments(body(selector.asTerm))
    if (path.isEmpty) {
      report.errorAndAbort("field(...) requires at least one field access, e.g. field[V](_.a).")
    }

    // Each segment's real type is only known once its TypeRepr is bound to a fresh type variable
    // via asType, so the fold has to happen inside that binding, one segment at a time. The final
    // Cur is, by construction, the same type as F (both come from typechecking the same selector
    // expression) — asExprOf checks that equality at macro-expansion time; it does not add any
    // runtime cast to the generated code.
    def buildFrom[Cur: Type](
      acc: Expr[ConditionExpression.Path[V, Cur]],
      remaining: List[(String, TypeRepr)]
    ): Expr[ConditionExpression.Path[V, F]] =
      remaining match {
        case Nil =>
          acc.asExprOf[ConditionExpression.Path[V, F]]
        case (segName, segTpe) :: rest =>
          segTpe.asType match {
            case '[segT] =>
              val next: Expr[ConditionExpression.Path[V, segT]] = '{ $acc.get[segT](${ Expr(segName) }) }
              buildFrom[segT](next, rest)
          }
      }

    val (headName, headTpe) = path.head
    headTpe.asType match {
      case '[headT] =>
        val base: Expr[ConditionExpression.Path[V, headT]] =
          '{ ConditionExpression.Attr[V, headT](${ Expr(headName) }) }
        buildFrom[headT](base, path.tail)
    }
  }
}
