package com.arslyte.experimental

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Test

class EvaluatorTest {

  private final val parser = new Parser()
  private final val compiler = new Compiler()
  private final val evaluator = new Evaluator()

  @Test
  def testEval(): Unit = {
    val userInput = """LOWER(ELEMENT_AT(SPLIT(col("key"), "\."), -1))"""
    val expr = this.parser.parse(userInput)
    val compiled = this.compiler.compile(expr)
    val fx = this.evaluator.eval(compiled)

    val row: Row = new GenericRowWithSchema(
      Array("1", "yoyo.mooooooooooooooooooooooooooooooooooo.like euh"),
      StructType(Seq(
        StructField("id", StringType),
        StructField("key", StringType)
      ))
    )
    println(fx(row))
  }

  @Test
  def testEvalUdf(): Unit = {
    val userInput = Map[String, String](
      "ABC" -> """LOWER(ELEMENT_AT(SPLIT(col("name"), "\."), -1))""",
      "BCEE" -> """LOWER(COL("key"))""",
      "331E" -> """XPATH(COL("xml"), "/root/beauty")""",
      "11e" -> """XPATH(COL("xml"), "/note/to")"""
    )
    val fxs: Map[String, Row => Option[String]] = userInput.map { case (key, value) =>
      val expr = this.parser.parse(value)
      println(expr)
      val compiled = this.compiler.compile(expr)
      println(compiled)
      val fx = this.evaluator.eval(compiled)
      println(fx)

      key -> fx
    }

    val spark = SparkSession
      .builder()
      .appName("arslyte.sandbox")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext.parallelize(Seq(
      ("1", "ABC", "ABC.BCD.EEEDP", null),
      ("2", "BCEE", "ABC.BCD.eega", null),
      ("3", "ABC", "ABC.BCD.DDEsw", null),
      ("4", "BCEE", "ABC.BCD.cs", null),
      ("5", "BCEE", "Edward.Yifeng.Liu", null),
      ("6", "NA", "Edward.YOhs.Liddd", null),
      ("6", "331E", "Edward.YOhs.dde2", "<root><beauty>10/10</beauty></root>"),
      ("7", "11e", "LOL.YESESES!", "<note><to>Tove</to><from>Jani</from><heading>Reminder</heading><body>Don't forget me this weekend!</body></note>")
    )).toDF("id", "key", "name", "xml")
    df.show(10, truncate = false)

    val tx = df.withColumn("value", Evaluator(fxs)(struct("*"), col("key")))
    tx.show(10, truncate = false)

    spark.stop()
  }
}
