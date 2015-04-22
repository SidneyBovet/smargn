package controllers

import com.decodified.scalassh
import com.decodified.scalassh.SSH
import com.decodified.scalassh.SSH._
import play.api.Logger
import play.api.libs.json._
import play.api.mvc._
import techniques.{DynamicTimeWrapping, NaiveComparisons, NaiveInverseComparisons, NaiveShiftComparison}
import utils._
import utils.Launcher._
import utils.Result
import utils.ResultParser

object Application extends Controller with ResultParser {

  private val INPUT = "input/"
  private val OUTPUT = "public/data/"

  private def createOutput(words: Seq[String], technique: String, params: Seq[Double]): String = {
    s"hdfs://projects/temporal-profiles/results/${words.mkString("-")}${
      if (params.nonEmpty) {
        s"_${technique.toLowerCase}_${
          params.mkString("-")
        }"
      } else {
        ""
      }
    }/"
  }

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
      val res = Launcher.run(word, INPUT, OUTPUT, List(4.0), NaiveInverseComparisons.naiveInverseDifference)
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
  def blank: Action[AnyContent] = {
    Action {
      Ok(views.html.smargn())
    }
  }

  /**
   * Parses the request as a Json and launches the right technique with the parameters on the words
   * @return HTTP Ok with results in a Json
   */
  def smargn: Action[JsValue] = {
    Action(BodyParsers.parse.json) { req =>
      bodyToJson(req.body) match {
        case Nil =>
          Logger.error(Json.prettyPrint(req.body))
          BadRequest("Json is not in the required format")
        case List(("words", Words(words)), ("technique", Name(name)), ("parameters", Parameters(params))) =>
          // Apply desired technique and get results
          // Create SSH connection to icdataportal2. Uses ~/.scala-ssh/icdataportal2 for authentication
          // Have a look at https://github.com/sirthias/scala-ssh#host-config-file-format to know what to put in it.
          SSH("icdataportal2") { client =>
            val client = scalassh.sshClientToRichClient(client)
            // Go to bash
            client.exec("bash")
            // Send the job to YARN with the correct arguments
            client.exec("spark-submit --class SparkCommander --master yarn-client SparkCommander-assembly-1.0.jar" +
              s"-w ${words.mkString(" ")}" +
              s"-t $name" +
              s"${
                if (params.nonEmpty) {
                  s"-p"
                } else {
                  ""
                }
              }")
            client.exec(s"hadoop fs -get ${createOutput(words, name, params)}/results.txt")
            client.download("~/results.txt", "./public/data/")
          }
          val results = name match {
            //  Add the case for your technique T here. Example:
            //  case T.name    => runList(words, <input dir>, <output dir>, parameters, <code for T>)
            case "Naive" => runList(words, INPUT, OUTPUT, params, NaiveComparisons.naiveDifferenceScalingMax)
            case "Inverse" => runList(words, INPUT, OUTPUT, params, NaiveInverseComparisons.naiveInverseDifference)
            case "Shift" => runList(words, INPUT, OUTPUT, params, NaiveShiftComparison.naiveDifferenceShift)
            case "DTW" => runList(words, INPUT, OUTPUT, params, DynamicTimeWrapping.dtwComparisonScaleMax)
            case _ => runList(words, INPUT, OUTPUT, params, NaiveComparisons.naiveDifferenceScalingMax)
          }

          // Send result to browser
          Ok(resultsToJson(results))
      }
    }
  }

  /**
   *
   * @param body the JSON body from the request
   * @return a map to the list of words that the user wants searched, the technique to use and its
   *         parameters
   */
  private def bodyToJson(body: JsValue): List[(String, Result)] = {
    body match {
      case JsObject(Seq(("words", JsArray(words)), ("technique", JsString(technique)),
      ("parameters", JsArray(params: Seq[JsString])))) =>
        // parsing array to list of words, technique name and parameters
        List(("words", Words(words)), ("technique", Name(technique)), ("parameters", Parameters(params)))
      case _ => Nil
    }
  }

  private def stdOutToMap(results: String): Map[String, List[String]] = {
    parse(result, results) match {
      case Success(map, _) => map
      case Failure(_, _) => Map()
      case Error(_, _) => Map()
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
    { case ((lNSW, lNID, lRES), (w, List(NSW))) => (w :: lNSW, lNID, lRES)
    case ((lNSW, lNID, lRES), (w, List(NOTFOUND))) => (lNSW, w :: lNID, lRES)
    case ((lNSW, lNID, lRES), (w, result)) => (lNSW, lNID, lRES + (w -> result))
    }

    Json.obj("nosimilarwords" -> Json.toJson(nsw), "notindata" -> Json.toJson(nid), "results" ->
      Json.toJson(res))
  }
}