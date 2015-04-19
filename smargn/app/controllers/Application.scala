package controllers

import play.api.Logger
import play.api.libs.json._
import play.api.mvc._
import techniques.{NaiveComparisons, NaiveInverseComparisons, NaiveShiftComparison}
import utils._
import utils.Launcher._
import utils.Result

object Application extends Controller {

  private val INPUT = "input/"
  private val OUTPUT = "public/data/"

  // Enables CORS
  val headers: List[(String, String)] = List("Access-Control-Allow-Origin" -> "*",
    "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS", "Access-Control-Max-Age" -> "3600",
    "Access-Control-Allow-Headers" -> "Origin, Content-Type, Accept, Authorization",
    "Access-Control-Allow-Credentials" -> "true")

  def options(url: String): Action[AnyContent] = {
    Action { request =>
      NoContent.withHeaders(headers: _*)
    }
  }

  def index: Action[AnyContent] = {
    Action {
      Ok(views.html.index("Hello world"))
    }
  }

  def runNaive(word: String): Action[AnyContent] = {
    Action {
      val res = run(word, INPUT, OUTPUT, List(0.2), NaiveComparisons.naiveDifferenceScalingMax)
      if (res == List()) {
        Ok(views.html.notSimilarWords(word))
      } else if (res.head == "ERROR404") {
        Ok(views.html.notFoundPage(word))
      } else {
        Ok(views.html.naive(res))
      }
    }
  }

  def runNaiveInverse(word: String): Action[AnyContent] = {
    Action {
      val res = Launcher.run(word, INPUT, OUTPUT, List(0.2), NaiveInverseComparisons.naiveInverseDifference)
      if (res == List()) {
        Ok(views.html.notSimilarWords(word))
      } else if (res.head == "ERROR404") {
        Ok(views.html.notFoundPage(word))
      } else {
        Ok(views.html.naive(res))
      }
    }
  }

  def runNaiveShift(word: String): Action[AnyContent] = {
    Action {
      val res = run(word, INPUT, OUTPUT, List(0.2), NaiveShiftComparison.naiveDifferenceShift)
      if (res == List()) {
        Ok(views.html.notSimilarWords(word))
      } else if (res.head == "ERROR404") {
        Ok(views.html.notFoundPage(word))
      } else {
        Ok(views.html.naive(res))
      }
    }
  }

  /**
   * Loads the default page
   * @return HTTP Ok with default html page
   */
  def blank: Action[AnyContent] = Action { Ok(views.html.smargn()) }

  /**
   * Parses the request as a Json and launches the right technique with the parameters on the words
   * @return HTTP Ok with results in a Json
   */
  def smargn: Action[JsValue] = Action(BodyParsers.parse.json) { req =>
    bodyToJson(req.body) match {
      case Nil =>
        Logger.error(Json.prettyPrint(req.body))
        BadRequest("Json is not in the required format")
      case List(("words", Words(words)), ("technique", Name(name)), ("parameters", Parameters(params))) =>
        // Apply desired technique and get results
        val results = name match {
          case "Naive"   => runList(words, INPUT, OUTPUT, params, NaiveComparisons.naiveDifferenceScalingMax)
          case "Inverse" => runList(words, INPUT, OUTPUT, params, NaiveInverseComparisons.naiveInverseDifference)
          case "Shift"   => runList(words, INPUT, OUTPUT, params, NaiveShiftComparison.naiveDifferenceShift)
          case _ => runList(words, INPUT, OUTPUT, params, NaiveComparisons.naiveDifferenceScalingMax)
        }

        // Send result to browser
        Ok(resultsToJson(results))
    }
  }

  /**
   *
   * @param body the JSON body from the request
   * @return a map to the list of words that the user wants searched, the technique to use and its parameters
   */
  private def bodyToJson(body: JsValue): List[(String, Result)] = {
    body match {
      case JsObject(Seq(("words",      JsArray(words)),
                        ("technique",  JsString(technique)),
                        ("parameters", JsArray(params: Seq[JsString])))) =>
        // parsing array to list of words, technique name and parameters
        List(("words",      Words(words)),
             ("technique",  Name(technique)),
             ("parameters", Parameters(params)))
      case _ => Nil
    }
  }

  /**
   *
   * @param results the results from the technique applied
   * @return a JSON object separating words into three categories:
   *         - Words that had no similar words (lNSW)
   *         - Words that were not in the data (lNID)
   *         - Words that had similar words together with their results (lRES)
   */
  private def resultsToJson(results: Map[String, List[String]]): JsValue = {
    // Compute words with no result, words not in the data and results for each words
    val (nsw, nid, res) = results.foldLeft((List[String](), List[String](), Map[String, List[String]]()))
    {
      case ((lNSW, lNID, lRES), (w, Nil)) => (w :: lNSW, lNID, lRES)
      case ((lNSW, lNID, lRES), (w, "ERROR404" :: Nil)) => (lNSW, w :: lNID, lRES)
      case ((lNSW, lNID, lRES), (w, result)) => (lNSW, lNID, lRES + (w -> result))
    }

    Json.obj("nosimilarwords" -> Json.toJson(nsw),
             "notindata"      -> Json.toJson(nid),
             "results"        -> Json.toJson(res))
  }
}