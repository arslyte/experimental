package com.arslyte.experimental

sealed trait Expr extends Serializable

object Expr extends Serializable {

  case class LitStr(s: String) extends Expr

  case class LitInt(i: Int) extends Expr

  case class VarCol(expr: Expr) extends Expr

  case class Upper(expr: Expr) extends Expr

  case class Lower(expr: Expr) extends Expr

  case class Split(expr: Expr, delimiterExpr: Expr) extends Expr

  case class ElementAt(expr: Expr, indexExpr: Expr) extends Expr
}
