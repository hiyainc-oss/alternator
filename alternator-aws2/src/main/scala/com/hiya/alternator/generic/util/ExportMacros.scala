package com.hiya.alternator.generic.util

import com.hiya.alternator.CompoundDynamoFormat
import com.hiya.alternator.generic.format.DerivedDynamoFormat

import scala.reflect.macros.blackbox

class ExportMacros(val c: blackbox.Context) {
  import c.universe._

  final def exportDynamoFormat[D[x] <: DerivedDynamoFormat[x], A](implicit
    D: c.WeakTypeTag[D[_]],
    A: c.WeakTypeTag[A]
  ): c.Expr[Exported[CompoundDynamoFormat[A]]] = {
    val target = appliedType(D.tpe.typeConstructor, A.tpe)

    c.typecheck(q"_root_.shapeless.lazily[$target]", silent = true) match {
      case EmptyTree => c.abort(c.enclosingPosition, s"Unable to infer value of type $target")
      case t =>
        c.Expr[Exported[CompoundDynamoFormat[A]]](
          q"new _root_.com.hiya.alternator.generic.util.Exported($t: _root_.com.hiya.alternator.CompoundDynamoFormat[$A])"
        )
    }
  }
}
