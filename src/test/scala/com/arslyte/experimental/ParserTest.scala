package com.arslyte.experimental

import org.junit.jupiter.api.Test

class ParserTest {

  private final val parser = new Parser()

  @Test
  def testParse(): Unit = {
    val userInput = """LOWER(ELEMENT_AT(SPLIT(COL("key"), ","), -1))"""
    val expr = this.parser.parse(userInput)
    println(expr)
  }
}
