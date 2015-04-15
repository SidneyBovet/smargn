package controllers

import techniques.{NaiveShiftComparison, NaiveInverseComparisons, NaiveComparisons}
import utils._
import play.api.mvc._

object Application extends Controller {

  def index: Action[AnyContent] = {
    Action {
      Ok(views.html.index("Hello world"))
    }
  }

  def runNaive(word: String) = {
    Action {
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

  def runNaiveInverse(word: String) = {
    Action {
      val res = Launcher.run(word, "input/", "public/data/", List(0.2), NaiveInverseComparisons.naiveInverseDifference)
      if (res == List()) {
        Ok(views.html.notSimilarWords(word))
      } else if (res(0) == "ERROR404") {
        Ok(views.html.notFoundPage(word))
      } else {
        Ok(views.html.naive(res))
      }
    }
  }

  def runNaiveShift(word: String) = {
    Action {
      val res = Launcher.run(word, "input/", "public/data/", List(0.2), NaiveShiftComparison.naiveDifferenceShift)
      if (res == List()) {
        Ok(views.html.notSimilarWords(word))
      } else if (res(0) == "ERROR404") {
        Ok(views.html.notFoundPage(word))
      } else {
        Ok(views.html.naive(res))
      }
    }
  }
}
