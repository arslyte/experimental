package com.arslyte.experimental

sealed trait Expr extends Serializable

object Expr extends Serializable {

  case class LitStr(s: String) extends Expr

  case class LitInt(i: Int) extends Expr

  case class VarCol(expr: Expr) extends Expr

  case class Upper(stringExpr: Expr) extends Expr

  case class Lower(stringExpr: Expr) extends Expr

  case class Split(stringExpr: Expr, delimiterExpr: Expr) extends Expr

  case class ElementAt(arrayExpr: Expr, indexExpr: Expr) extends Expr

  case class XPath(xmlExpr: Expr, pathExpr: Expr) extends Expr
}
