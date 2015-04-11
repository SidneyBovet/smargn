package controllers

import techniques.NaiveComparison
import techniques._
import utils._
import play.api.mvc._

object Application extends Controller {

  def index: Action[AnyContent] = {
    Action {
      Ok(views.html.index("Hello world"))
    }
  }

  def runNaive(word: String) = Action {
    val res = Launcher.run(word, "input/", "public/data/", List(0.2), NaiveComparisons.naiveDifferenceScalingMax)
    if (res == List()) {
      Ok(views.html.notSimilarWords(word))
    } else if (res(0) == "ERROR404") {
      Ok(views.html.notFoundPage(word))
    } else {
      Ok(views.html.naive(res))
    }
  }
}
