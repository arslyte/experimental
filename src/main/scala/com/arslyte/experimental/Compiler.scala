package com.arslyte.experimental

class Compiler extends Serializable {

  // define how to compile an <expr>
  def compile(expr: Expr): Compiled = expr match {
    case Expr.LitStr(s) => Compiled.LitStr(s)
    case Expr.LitInt(i) => Compiled.LitInt(i)
    case Expr.VarCol(expr) =>
      val compiled = compile(expr)
      Compiled.Call("col", Seq[Compiled](compiled))
    case Expr.Upper(expr) =>
      val compiled = compile(expr)
      Compiled.Call("upper", Seq[Compiled](compiled))
    case Expr.Lower(expr) =>
      val compiled = compile(expr)
      Compiled.Call("lower", Seq[Compiled](compiled))
    case Expr.Split(expr, delimiterExpr) =>
      val compiled = compile(expr)
      val delimiterCompiled = compile(delimiterExpr)
      Compiled.Call("split", Seq[Compiled](compiled, delimiterCompiled))
    case Expr.ElementAt(expr, indexExpr) =>
      val compiled = compile(expr)
      val indexCompiled = compile(indexExpr)
      Compiled.Call("element_at", Seq[Compiled](compiled, indexCompiled))
    case _ => throw new IllegalArgumentException(s"Unknown expr: ${expr}")
  }
}
