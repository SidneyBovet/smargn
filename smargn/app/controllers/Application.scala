package controllers

import play.api.Logger
import play.api.libs.json._
import play.api.mvc._
import techniques.{NaiveComparisons, NaiveInverseComparisons, NaiveShiftComparison}
import utils._

object Application extends Controller {

  // Enables CORS
  val headers = List(
    "Access-Control-Allow-Origin" -> "*",
    "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS",
    "Access-Control-Max-Age" -> "3600",
    "Access-Control-Allow-Headers" -> "Origin, Content-Type, Accept, Authorization",
    "Access-Control-Allow-Credentials" -> "true"
  )

  def options(url: String) = Action { request =>
    NoContent.withHeaders(headers : _*)
  }

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

  def runNaiveInverse(word: String) = Action {
    val res = Launcher.run(word, "input/", "public/data/", List(0.2), NaiveInverseComparisons.naiveInverseDifference)
    if (res == List()) {
      Ok(views.html.notSimilarWords(word))
    } else if (res(0) == "ERROR404") {
      Ok(views.html.notFoundPage(word))
    } else {
      Ok(views.html.naive(res))
    }
  }

  def runNaiveShift(word: String) = Action {
    val res = Launcher.run(word, "input/", "public/data/", List(0.2, 5.0), NaiveShiftComparison.naiveDifferenceScalingMax)
    if (res == List()) {
      Ok(views.html.notSimilarWords(word))
    } else if (res(0) == "ERROR404") {
      Ok(views.html.notFoundPage(word))
    } else {
      Ok(views.html.naive(res))
    }
  }

  def blankNaive = Action {
    Ok(views.html.naive(Nil))
  }

  def naive = Action(parse.json) { req =>
    bodyToJson(req.body) match {
      case Nil =>
        Logger.debug(Json.prettyPrint(req.body))
        BadRequest("Expected Json")
      case words =>
        // Get results from technique
        val results = Launcher.runList(words, "input/", "public/data/", List(0.2), NaiveComparisons.naiveDifferenceScalingMax)

        // Send result to browser
        Ok(resultsToJson(results))
    }
  }

  /**
   *
   * @param body the JSON body from the request
   * @return the list of words that the user wants searched
   */
  private def bodyToJson(body: JsValue): List[String] = body match {
    case JsObject(fields) =>
        // parsing array to list of words
        val jsonedWords = Json.arr(fields.head._2).as[List[String]]
        jsonedWords.flatMap(allWords => allWords.substring(1, allWords.length - 1).split(",").map(w => w.substring(1, w.length - 1)))
    case _ => Nil
  }

  /**
   *
   * @param results the results from the technique applied
   * @return a JSON object separating words into three categories:
   *         - Words that had no similar words
   *         - Words that were not in the data
   *         - Words that had similar words together with their results
   */
  private def resultsToJson(results: Map[String, List[String]]): JsValue = {
    // Compute words with no result, words not in the data and results for each words
    val tripletRes = results.foldLeft((List[String](), List[String](), Map[String, List[String]]())) {
      case ((lNSW, lNID, lRES), (w, Nil)) => (w :: lNSW, lNID, lRES)
      case ((lNSW, lNID, lRES), (w, "ERROR404" :: Nil)) => (lNSW, w :: lNID, lRES)
      case ((lNSW, lNID, lRES), (w, res)) => (lNSW, lNID, lRES + (w -> res))
    }

    Json.obj("nosimilarwords" -> Json.toJson(tripletRes._1),
             "notindata" -> Json.toJson(tripletRes._2),
             "results" -> Json.toJson(tripletRes._3))
  }
}
