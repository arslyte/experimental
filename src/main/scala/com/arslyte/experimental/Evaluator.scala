package com.arslyte.experimental

import kantan.xpath.implicits._
import kantan.xpath.{DecodeResult, Query}
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

  private def xpath(xml: String, path: Query[DecodeResult[String]]): Option[String] = Try {
    xml.unsafeEvalXPath[String](path)
  }.toOption

  // define how to evaluate a <compiled> stack
  private def evaluate(compiled: Compiled): Row => Option[Any] = (row: Row) => {
    compiled match {
      case Compiled.LitStr(s) => Some(s)
      case Compiled.LitInt(i) => Some(i)
      case Compiled.LitXPath(xpath) => Some(xpath)
      case Compiled.Call(name, args) =>
        name match {
          case "col" =>
            val name = evaluate(args.head)(row).orNull.asInstanceOf[String]
            col(row, name)
          case "upper" =>
            val string = evaluate(args.head)(row).orNull.asInstanceOf[String]
            upper(string)
          case "lower" =>
            val string = evaluate(args.head)(row).orNull.asInstanceOf[String]
            lower(string)
          case "split" =>
            val string = evaluate(args.head)(row).orNull.asInstanceOf[String]
            val delimiter = evaluate(args(1))(row).orNull.asInstanceOf[String]
            split(string, delimiter)
          case "element_at" =>
            val array = evaluate(args.head)(row).orNull.asInstanceOf[Array[Any]]
            val index = evaluate(args(1))(row).orNull.asInstanceOf[Int]
            elementAt(array, index)
          case "xpath" =>
            val xml = evaluate(args.head)(row).orNull.asInstanceOf[String]
            val path = evaluate(args(1))(row).orNull.asInstanceOf[Query[DecodeResult[String]]]
            xpath(xml, path)
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