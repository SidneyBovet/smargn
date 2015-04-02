package controllers

import techniques.NaiveComparison
import play.api.mvc._

object Application extends Controller {

  def index: Action[AnyContent] = {
    Action {
      Ok(views.html.index("Hello world"))
    }
  }

  def runNaive(word: String) = Action {
    val res = NaiveComparison.run(word, "input/", "public/data/")
    if(res == Nil) {
      Ok(views.html.notFoundPage(word))
    } else {
      Ok(views.html.naive(res))
    }
  }
}
