package controllers

import java.io.File
import java.nio.file.{FileSystems, Files}

import com.decodified.scalassh.SSH
import play.api.Logger
import play.api.libs.json._
import play.api.mvc._
import techniques.{NaiveComparisons, NaiveInverseComparisons, NaiveShiftComparison}
import utils.Launcher._
import utils.{Result, ResultParser, _}

import scala.io.Source

object Application extends Controller with ResultParser {

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

  def getCSV(search: String): Action[AnyContent] = {
    Action(Ok.sendFile(new File("./public/results/" + search + "/data.csv")))
  }

  /**
   * Loads the default page
   * @return HTTP Ok with default html page
   */
  def blank: Action[AnyContent] = {
    Action(Ok(views.html.smargn()))
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
          val paramsStr = if (params.nonEmpty) s"_${params.mkString("-")}" else ""
          val outputDir = s"${words.mkString("-")}_${name.toLowerCase}$paramsStr"

          val resultsPath = FileSystems.getDefault.getPath(s"./public/results/$outputDir/")
          if (Files.notExists(resultsPath)) {
            Files.createDirectory(resultsPath)
          }
          SSH("icdataportal2") { client =>
            Logger.debug("Connection to icdataportal2 complete")
            val hdfsResDir = "/projects/temporal-profiles/results/" + outputDir

            // TODO before sending the job to YARN, check if the directory already exists on HDFS
            // Send the job to YARN with the correct arguments
            // At each step, exit code 0 means success. All others mean failure.
            // See http://support.attachmate.com/techdocs/2116.html for more details on exit codes
            Logger.debug(s"Send job to YARN $hdfsResDir")
            client.exec(
              "bash -c \"source .bashrc; spark-submit --class SparkCommander --master yarn-cluster --num-executors 25" +
                " --executor-cores 2 SparkCommander-assembly-1.0.jar -w " + words.mkString(",") + " -t " + name + {
                if (params.nonEmpty) {
                  " -p " + params.mkString(",")
                } else {
                  ""
                }
              } + "\"").right.flatMap { res =>
              Logger.debug("Job finished: " + res.exitCode.get)

              //Make the directory usable by others in the group
              client.exec("bash -c \"source .bashrc; hadoop fs -chmod -R 775 hdfs://" + hdfsResDir + "\"").right
                .flatMap { res =>
                Logger.debug("chmod done " + res.exitCode.get)

                // Download results from HDFS to local on cluster
                client.exec("bash -c \"source .bashrc; hadoop fs -getmerge " + hdfsResDir + "/results ~/results.txt\"")
                  .right.flatMap { res =>
                  Logger.debug("get results.txt done " + res.exitCode.get)
                  // Download results from cluster to server
                  client.download("results.txt", "./public/results/" + outputDir).right.map { res =>
                    Logger.debug("results.txt downloaded to " + outputDir)

                    // Download data.csv from HDFS to local on cluster
                    client.exec("bash -c \"source .bashrc; hadoop fs -getmerge " + hdfsResDir + "/data ~/data.csv\"")
                      .right.flatMap { res =>
                      Logger.debug("get data.csv done " + res.exitCode.get)

                      // Download data.csv from cluster to server
                      client.download("data.csv", "./public/results/" + outputDir).right.map { res =>
                        Logger.debug("data.csv downloaded to " + outputDir)
                        // Remove results and data frorm local account on the cluster
                        client.exec("rm results.txt data.csv").right
                          .map { res => Logger.debug("Deleted results.txt and data.csv from local on the cluster")

                          // Read downloaded result file
                          val resultsStream = Source.fromFile("./public/results/" + outputDir + "/results.txt", "utf-8")
                          val resultsStr = resultsStream.getLines().toList
                          resultsStream.close()
                          // Send back results to the browser
                          Ok(resultsToJson(stdOutToMap(resultsStr)))
                        }
                      }
                    }
                  }
                }
              }
            } // up up down down left right left right B A start
          }.right.get.right.get.right.get
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

  private def stdOutToMap(results: List[String]): Map[String, List[String]] = {
    //    Logger.debug("Results are: " + results)
    results.flatMap(e => parse(result, e).get).toMap
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
