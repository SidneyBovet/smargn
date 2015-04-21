package utils

import scala.util.parsing.combinator._

/**
 * Created by Valentin on 21/04/15.
 */
trait ResultParser extends RegexParsers {
  val NSW = "NOSIMILARWORDS"
  val NOTFOUND = "ERROR404"

  def result: Parser[Map[String, List[String]]] = {
    rep(line) ^^ { case l => l.foldLeft(Map[String, List[String]]()) { case (acc, curr) => acc ++ curr } }
  }

  def line: Parser[Map[String, List[String]]] = {
    word ~ "->" ~ (error | rep(word)) ~ "\n" ^^ { case w ~ sep ~ NSW ~ end => Map(w -> List(NSW))
    case w ~ sep ~ NOTFOUND ~ end => Map(w -> List(NOTFOUND))
    case w ~ sep ~ (ws: List[String]) ~ end => Map(w -> ws)
    }
  }

  def word: Parser[String] = {
    """[a-zA-Z04]""".r ^^ {
      _.toString
    }
  }

  def error: Parser[String] = NSW | NOTFOUND
}