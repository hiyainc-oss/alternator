package com.hiya.alternator.generic.util

import com.hiya.alternator.syntax.ConditionExpression

import scala.reflect.macros.blackbox

class FieldSelectorMacros(val c: blackbox.Context) {
  import c.universe._

  def field[V: c.WeakTypeTag, F: c.WeakTypeTag](selector: c.Expr[V => F]): c.Expr[ConditionExpression.Path[V, F]] = {

    // Each Select node is already fully typechecked by the time the macro runs (selector's
    // static type is V => F), so `tree.tpe` gives the real, precise type of that field —
    // no need to fall back to Any and cast at the end.
    def segments(tree: Tree): List[(String, Type)] = tree match {
      case Select(inner, name) => segments(inner) :+ ((name.decodedName.toString, tree.tpe.widen))
      case Ident(_) => Nil
      case _ =>
        c.abort(
          c.enclosingPosition,
          s"field(...) requires a plain field-accessor chain, e.g. field[${weakTypeOf[V]}](_.a.b). Got: ${showRaw(selector.tree)}"
        )
    }

    val path = selector.tree match {
      case Function(List(_), body) => segments(body)
      case _ =>
        c.abort(c.enclosingPosition, s"field(...) requires a lambda literal, e.g. field[${weakTypeOf[V]}](_.a.b).")
    }

    if (path.isEmpty) {
      c.abort(c.enclosingPosition, "field(...) requires at least one field access, e.g. field[V](_.a).")
    }

    // No explicit field-existence check needed: `selector.tree` only reaches this point already
    // typechecked as V => F, so every segment (head and nested) is already a real, accessible
    // member of its receiver — the ordinary Scala typer rejects `_.nonexistent` before macro
    // expansion even begins.
    val vTpe = weakTypeOf[V]
    val (headName, headTpe) = path.head

    val base: Tree = q"_root_.com.hiya.alternator.syntax.ConditionExpression.Attr[$vTpe, $headTpe]($headName)"
    val fullTree = path.tail.foldLeft(base) { case (acc, (segment, segTpe)) =>
      q"$acc.get[$segTpe]($segment)"
    }

    c.Expr[ConditionExpression.Path[V, F]](fullTree)
  }
}
