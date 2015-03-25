package controllers

import ngrams.Test
import techniques.NaiveComparison
import play.api._
import play.api.mvc._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Hello world"))
  }

  def test = Action {
    val v = new Test("I <3 Ngrams")
    Ok(views.html.test(v.getMsg))
  }

  def defaultTest = Action {
    val v = new Test("dont care")
    Ok(views.html.test(v.getDefaultMsg))
  }

  def runNaive = Action {
    val res = NaiveComparison.run("input/", "output/")
    Ok(views.html.naive(res))
  }
}