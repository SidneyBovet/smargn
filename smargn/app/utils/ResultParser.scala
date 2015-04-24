package utils

import scala.util.parsing.combinator._

/**
 * Created by Valentin on 21/04/15.
 */
trait ResultParser extends RegexParsers {
  val NSW = "NOSIMILARWORDS"
  val NOTFOUND = "ERROR404"

  def result: Parser[Map[String, List[String]]] = {
    rep((line | errorLine) <~ opt("\n")) ^^ { case l => l.flatten.toMap
    }
  }

  def line: Parser[Map[String, List[String]]] = {
    (word <~ "->") ~ rep(word) ^^ { case w ~ ws => Map(w -> ws)
    }
  }

  def errorLine: Parser[Map[String, List[String]]] = {
    (word <~ "->") ~ error ^^ { case w ~ NSW => Map(w -> List(NSW))
    case w ~ NOTFOUND => Map(w -> List(NOTFOUND))
    }
  }

  // Darn you accents!
  def word: Parser[String] = {
    "[a-zA-Z0-9âàáêèéîìíôòóûùúÂÀÁÊÈÉÎÌÍÔÒÓÛÙÚ']+".r ^^ { case w => w.toString }
  }

  def error: Parser[String] = NSW | NOTFOUND
}