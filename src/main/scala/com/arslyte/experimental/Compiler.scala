package com.arslyte.experimental

import kantan.xpath.Query

class Compiler extends Serializable {

  // define how to compile an <expr>
  def compile(expr: Expr): Compiled = expr match {
    case Expr.LitStr(s) => Compiled.LitStr(s)
    case Expr.LitInt(i) => Compiled.LitInt(i)
    case Expr.VarCol(expr) =>
      val name = compile(expr)
      Compiled.Call("col", Seq[Compiled](name))
    case Expr.Upper(stringExpr) =>
      val string = compile(stringExpr)
      Compiled.Call("upper", Seq[Compiled](string))
    case Expr.Lower(stringExpr) =>
      val string = compile(stringExpr)
      Compiled.Call("lower", Seq[Compiled](string))
    case Expr.Split(stringExpr, delimiterExpr) =>
      val string = compile(stringExpr)
      val delimiter = compile(delimiterExpr)
      Compiled.Call("split", Seq[Compiled](string, delimiter))
    case Expr.ElementAt(arrayExpr, indexExpr) =>
      val array = compile(arrayExpr)
      val index = compile(indexExpr)
      Compiled.Call("element_at", Seq[Compiled](array, index))
    case Expr.XPath(xmlExpr, pathExpr) =>
      val xml = compile(xmlExpr)
      val path = Compiled.LitXPath(
        {
          val litStr = compile(pathExpr).asInstanceOf[Compiled.LitStr]
          val query = Query.compile[String](litStr.s)

          query match {
            case Right(path) => path
            case _ => throw new IllegalArgumentException(s"Invalid XPath query: ${litStr.s}")
          }
        })
      Compiled.Call("xpath", Seq[Compiled](xml, path))
    case _ => throw new IllegalArgumentException(s"Unknown expr: ${expr}")
  }
}
