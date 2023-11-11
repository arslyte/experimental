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
      "BCEE" -> """LOWER(COL("key"))"""
    )
    val fxs: Map[String, Row => Option[String]] = userInput.map { case (key, value) =>
      val expr = this.parser.parse(value)
      val compiled = this.compiler.compile(expr)
      val fx = this.evaluator.eval(compiled)

      key -> fx
    }

    val spark = SparkSession
      .builder()
      .appName("arslyte.sandbox")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext.parallelize(Seq(
      ("1", "ABC", "ABC.BCD.EEEDP"),
      ("2", "BCEE", "ABC.BCD.eega"),
      ("3", "ABC", "ABC.BCD.DDEsw"),
      ("4", "BCEE", "ABC.BCD.cs"),
      ("5", "BCEE", "Edward.Yifeng.Liu"),
      ("6", "NA", "Edward.YOhs.Liddd")
    )).toDF("id", "key", "name")
    df.show(10, truncate = false)

    val tx = df.withColumn("value", Evaluator(fxs)(struct("*"), col("key")))
    tx.show(10, truncate = false)

    spark.stop()
  }
}
