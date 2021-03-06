package utils

import play.api.libs.json._

/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * Created by Valentin on 19/04/15.
 */

sealed trait Result


case class Words(words: List[String]) extends Result {
  protected def this(words: Seq[JsValue]) = this(words.toList.map(_.as[String]))
}

object Words {
  def apply(words: Seq[JsValue]): Words = new Words(words)
}


case class Name(name: String) extends Result

case class Parameters(params: List[Double]) extends Result {
  protected def this(params: Seq[JsString]) = this(params.toList.map { case JsString(w) => w.toDouble })
}

object Parameters {
  def apply(params: Seq[JsString]): Parameters = new Parameters(params)
}

case class Range_(range: Range) extends Result

object Range_ {
  def apply(startY: Int, endY:Int): Range_ = new Range_(startY to endY)
}