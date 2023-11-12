package com.arslyte.experimental

class Parser extends Serializable {

  // define helper entities & functions
  private case class Fn(name: String, args: Seq[String]) extends Serializable

  private final val functionRegex = "(\\w+)\\((.*)\\)"
  private final val functionArgumentRegex = """,(?![^(]*\))(?![^"']*["'](?:[^"']*["'][^"']*["'])*[^"']*$)"""
  private final val litStrRegex = "\"([^\"]*)\""
  private final val litIntRegex = "^-?\\d+$"

  // define how to parse <userInput>
  def parse(userInput: String): Expr = {
    def parseParentheses(userInput: String): Fn = {
      val (fnName, remainder) = if (userInput.matches(this.functionRegex)) {
        (
          this.functionRegex.r.replaceAllIn(userInput, "$1").trim,
          this.functionRegex.r.replaceAllIn(userInput, "$2").trim
        )
      } else ("", userInput.trim)
      val fnArgs = remainder.split(this.functionArgumentRegex).map(_.trim)

      Fn(fnName, fnArgs)
    }

    def parseExpr(partialInput: String): Expr = {
      val parentheses = parseParentheses(partialInput)
      val argument = parseFn(parentheses)

      argument
    }

    def parseFn(fn: Fn): Expr = {
      if (fn.name.isEmpty) {
        fn.args.head match {
          case litStr if litStr.matches(this.litStrRegex) =>
            return Expr.LitStr(this.litStrRegex.r.replaceAllIn(litStr, "$1"))
          case litInt if litInt.matches(this.litIntRegex) => return Expr.LitInt(litInt.toInt)
          case _ => throw new IllegalArgumentException(s"Unknown literal: ${fn.args.head}")
        }
      }

      fn.name.toLowerCase match {
        case "col" => Expr.VarCol(parseExpr(fn.args.head))
        case "upper" => Expr.Upper(parseExpr(fn.args.head))
        case "lower" => Expr.Lower(parseExpr(fn.args.head))
        case "split" => Expr.Split(
          parseExpr(fn.args.head),
          parseExpr(fn.args(1)))
        case "element_at" => Expr.ElementAt(
          parseExpr(fn.args.head),
          parseExpr(fn.args(1)))
        case "xpath" => Expr.XPath(
          parseExpr(fn.args.head),
          parseExpr(fn.args(1)))
        case _ => throw new IllegalArgumentException(s"Unknown function: ${fn.name}")
      }
    }

    parseExpr(userInput)
  }
}
