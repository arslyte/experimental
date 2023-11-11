package com.arslyte.experimental

sealed trait Compiled extends Serializable

object Compiled extends Serializable {

  case class LitStr(s: String) extends Compiled

  case class LitInt(i: Int) extends Compiled

  case class Call(name: String, args: Seq[Compiled]) extends Compiled
}
