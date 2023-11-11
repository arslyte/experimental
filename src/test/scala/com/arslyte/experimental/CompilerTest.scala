package com.arslyte.experimental

import org.junit.jupiter.api.Test

class CompilerTest {

  private final val parser = new Parser()
  private final val compiler = new Compiler()

  @Test
  def testCompile(): Unit = {
    val userInput = """LOWER(ELEMENT_AT(SPLIT(COL("key"), ","), -1))"""
    val expr = this.parser.parse(userInput)
    val compiled = this.compiler.compile(expr)
    println(compiled)
  }
}
