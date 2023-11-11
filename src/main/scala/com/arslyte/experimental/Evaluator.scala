package com.arslyte.experimental

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

class Evaluator extends Serializable {

  // define available functions
  private def col(row: Row, name: String): Option[String] = Try {
    row.getAs[String](name)
  }.toOption

  private def elementAt[T](array: Array[T], index: Int): Option[T] = Try {
    val length = array.length
    if (length == 0 || length <= index || length < index.abs) return None
    if (index >= 0) array(index)
    else array(length + index)
  }.toOption

  private def lower(string: String): Option[String] = Try {
    string.toLowerCase
  }.toOption

  private def split(string: String, delimiter: String): Option[Array[String]] = Try {
    string.split(delimiter)
  }.toOption

  private def upper(string: String): Option[String] = Try {
    string.toUpperCase
  }.toOption

  // define how to evaluate a <compiled> stack
  private def evaluate(compiled: Compiled): Row => Option[Any] = (row: Row) => {
    compiled match {
      case Compiled.LitStr(s) => Some(s)
      case Compiled.LitInt(i) => Some(i)
      case Compiled.Call(name, args) =>
        name match {
          case "col" =>
            val evaluated = evaluate(args.head)(row).orNull.asInstanceOf[String]
            col(row, evaluated)
          case "upper" =>
            val evaluated = evaluate(args.head)(row).orNull.asInstanceOf[String]
            upper(evaluated)
          case "lower" =>
            val evaluated = evaluate(args.head)(row).orNull.asInstanceOf[String]
            lower(evaluated)
          case "split" =>
            val evaluated = evaluate(args.head)(row).orNull.asInstanceOf[String]
            val delimiterEvaluated = evaluate(args(1))(row).orNull.asInstanceOf[String]
            split(evaluated, delimiterEvaluated)
          case "element_at" =>
            val evaluated = evaluate(args.head)(row).orNull.asInstanceOf[Array[Any]]
            val indexEvaluated = evaluate(args(1))(row).orNull.asInstanceOf[Int]
            elementAt(evaluated, indexEvaluated)
        }
    }
  }

  def eval(compiled: Compiled): Row => Option[String] = evaluate(compiled).asInstanceOf[Row => Option[String]]
}

object Evaluator extends Serializable {

  private def evaluate(row: Row, key: String)
                      (implicit fxs: Map[String, Row => Option[String]]): Option[String] =
    Try {
      val fx = fxs.get(key).orNull
      fx(row).orNull
    }.toOption

  def apply(implicit fxs: Map[String, Row => Option[String]]): UserDefinedFunction =
    udf { (row: Row, key: String) => this.evaluate(row, key) }

}