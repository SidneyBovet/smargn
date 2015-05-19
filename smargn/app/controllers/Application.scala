package controllers

import java.io.File
import java.nio.file.{FileSystems, Files}

import com.decodified.scalassh.{CommandResult, SSH, SshClient, Validated}
import play.api.Logger
import play.api.libs.json._
import play.api.mvc._
import utils.{Result, ResultParser, _}

import scala.io.Source

/*
 * Contributors:
 *  - Valentin Rutz: all
 */
object Application extends Controller with ResultParser {

  private val INPUT = "input/"
  private val OUTPUT = "public/data/"
  private val FIRST_LINE = "Word,Year,Occurrences"

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

  def displayBlank: Action[AnyContent] = {
    Action {
      Ok(views.html.display())
    }
  }

  def displayCurve: Action[JsValue] = {
    Logger.debug("Trying to display a curve")
    Action(BodyParsers.parse.json) { req =>
      req.body match {
        case JsObject(Seq(("words", JsArray(words)))) =>
          val localFolder = MD5.hash(words.map(_.as[String]).mkString("-"))
          val resultsPath = FileSystems.getDefault.getPath(s"./public/results/$localFolder/")
          if (Files.notExists(resultsPath)) {
            Files.createDirectory(resultsPath)
          }

          val hdfsResDir = "/projects/temporal-profiles/results/" + localFolder
          val res = SSH("icdataportal2") { client =>
            Logger.debug("Connection to icdataportal2 complete")

            val r = client.exec("hadoop fs -test -d " + hdfsResDir + "; echo $?")
            r.right.map { res =>
              Logger.debug("Does " + hdfsResDir + " exist? " + res.stdOutAsString().charAt(0))
              if (res.stdOutAsString().charAt(0) == '0') {
                // Read from HDFS
                mergeAndDl(hdfsResDir, "~/data.csv", "./public/results/" + localFolder)(client).right.map { res =>
                  Ok(localFolder)
                }
              } else {
                sparkSubmitDisplayer(words.map(Json.stringify).toList, hdfsResDir, localFolder)(client).right
                  .flatMap { res =>
                  Logger.debug("Chmod done: " + res.exitCode.get)
                  mergeAndDl(hdfsResDir, "~/data.csv", "./public/results/" + localFolder)(client).right.map { res =>
                    Ok(localFolder)
                  }
                }
              }
            }
          }
          res.right.get.right.get
        case _ => BadRequest("Json is not in the good format")
      }
    }
  }

  def getCSV(search: String): Action[AnyContent] = {
    Action {
      Logger.debug(search)
      val resultPath = search
      Logger.debug("Retrieving data for display on search: " + resultPath)
      val dataCSVFile = new File("./public/results/" + resultPath + "/complete_data.csv")
      val dataCSV = Source.fromFile("./public/results/" + resultPath + "/data.csv").getLines().toList
      val file = Ok.sendFile(if (dataCSV == Nil) {
        printToFile(dataCSVFile) { p => p.println(FIRST_LINE) }
        dataCSVFile
      } else if (dataCSV.head != FIRST_LINE) {
        Logger.debug("first line was " + dataCSV.head)
        printToFile(dataCSVFile) { p => (FIRST_LINE :: dataCSV).foreach(p.println) }
        dataCSVFile
      } else {
        new File("./public/results/" + resultPath + "/data.csv")
      })
      rmLocalCopies(resultPath)
      file
    }
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
        case List(("words", Words(words)), ("technique", Name(name)), ("parameters", Parameters(params)),
        ("range", Range_(range))) =>
          // Apply desired technique and get results
          // Create SSH connection to icdataportal2. Uses ~/.scala-ssh/icdataportal2 for authentication
          // Have a look at https://github.com/sirthias/scala-ssh#host-config-file-format to know what to put
          // in it.
          val paramsStr = if (params.nonEmpty) s"_${params.mkString("-")}" else ""
          val outputDir = MD5.hash(s"${words.mkString("-")}_${name.toLowerCase}_${range.start}-${range.end}$paramsStr")

          val resultsPath = FileSystems.getDefault.getPath(s"./public/results/$outputDir/")
          if (Files.notExists(resultsPath)) {
            Files.createDirectory(resultsPath)
          }
          val res = SSH("icdataportal2") { client =>
            Logger.debug("Connection to icdataportal2 complete")
            val hdfsResDir = "/projects/temporal-profiles/results/" + outputDir

            val r = client.exec("hadoop fs -test -d " + hdfsResDir + "/results; echo $?")
            r.right.map { res =>
              Logger.debug("Does " + hdfsResDir + " exist? " + res.stdOutAsString().charAt(0))
              if (res.stdOutAsString().charAt(0) == '0') {
                // Read from HDFS
                mergeAndDl(hdfsResDir + "/results", "~/results.txt", "./public/results/" + outputDir)(client).right
                  .map { res =>
                  Logger.debug("results.txt downloaded to " + outputDir)
                  mergeAndDl(hdfsResDir + "/data", "~/data.csv", "./public/results/" + outputDir)(client).right
                    .map { res =>
                    Logger.debug("results.txt downloaded to " + outputDir)
                    // Remove results and data frorm local account on the cluster
                    client.exec("rm results.txt data.csv").right
                      .map { res => Logger.debug("Deleted results.txt and data.csv from local on the cluster")

                      // Read downloaded result file
                      val resultsStream = Source.fromFile("./public/results/" + outputDir + "/results.txt", "utf-8")
                      val resultsStr = resultsStream.getLines().toList
                      resultsStream.close()
                      // Send back results to the browser
                      Ok(resultsToJson(stdOutToMap(resultsStr), outputDir))
                    }
                  }
                }.right.get.right.get.right.get
              } else {
                // first spark-submit, then read form HDFS
                // Send the job to YARN with the correct arguments
                // At each step, exit code 0 means success. All others mean failure.
                // See http://support.attachmate.com/techdocs/2116.html for more details on exit codes
                Logger.debug(s"Send job to YARN $hdfsResDir")
                sparkSubmit(words, name, params, range, hdfsResDir, outputDir)(client).right.map { res =>
                  Logger.debug("chmod done: " + res.exitCode.get)
                  mergeAndDl(hdfsResDir + "/results", "~/results.txt", "./public/results/" + outputDir)(client).right
                    .map { res =>
                    Logger.debug("results.txt downloaded to " + outputDir)
                    mergeAndDl(hdfsResDir + "/data", "~/data.csv", "./public/results/" + outputDir)(client).right
                      .map { res =>
                      Logger.debug("results.txt downloaded to " + outputDir)
                      // Remove results and data frorm local account on the cluster
                      client.exec("rm results.txt data.csv").right.map { res =>
                        Logger.debug("Deleted results.txt and data.csv from local on the cluster")

                        // Read downloaded result file
                        val resultsStream = Source.fromFile("./public/results/" + outputDir + "/results.txt", "utf-8")
                        val resultsStr = resultsStream.getLines().toList
                        resultsStream.close()
                        // Send back results to the browser
                        Ok(resultsToJson(stdOutToMap(resultsStr), outputDir))
                      }
                    }
                  } // up up down down left right left right B A start
                }.right.get.right.get.right.get.right.get
              }
            }
          }
          res.right.get
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
      case JsObject(
      Seq(("words", JsArray(words)), ("technique", JsString(technique)), ("parameters", JsArray(params: Seq[JsString])),
      ("range", JsObject(Seq(("start", JsString(startYear)), ("end", JsString(endYear))))))) =>
        Logger.debug("" + startYear)
        Logger.debug("" + endYear)
        Logger.debug(words.map(_.as[String]).mkString(" ") + "-" + technique + "-" + params.mkString(" "))
        // parsing array to list of words, technique name and parameters
        List(("words", Words(words)), ("technique", Name(technique.toLowerCase)), ("parameters", Parameters(params)),
          ("range", Range_(startYear.toInt, endYear.toInt)))
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
  private def resultsToJson(results: Map[String, List[String]], hash: String): JsValue = {
    // Compute words with no result, words not in the data and results for each words
    //    Logger.debug("Results are: " + results)
    val (nsw, nid, res) = results.foldLeft((List[String](), List[String](), Map[String, List[String]]()))
    { case ((lNSW, lNID, lRES), (w, List(NSW))) => (w :: lNSW, lNID, lRES)
    case ((lNSW, lNID, lRES), (w, List(NOTFOUND))) => (lNSW, w :: lNID, lRES)
    case ((lNSW, lNID, lRES), (w, result)) => (lNSW, lNID, lRES + (w -> result))
    }

    Json.obj("nosimilarwords" -> Json.toJson(nsw), "notindata" -> Json.toJson(nid), "results" ->
      Json.toJson(res), "hash" -> Json.toJson(hash))
  }

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def sparkSubmit(words: List[String], name: String, params: List[Double], range: Range, hdfsResDir: String,
                  hash: String)(implicit client: SshClient): Validated[CommandResult] = {
    Logger.debug(name.toLowerCase)
    client.exec("bash -c \"source .bashrc; spark-submit --class SparkCommander --master yarn-cluster " +
      "--num-executors 25 SparkCommander-assembly-1.0.jar -h " + hash + " -w " + words.mkString(",") + " -t " +
      name.toLowerCase +
      " -r " + range.start + "," + range.end + {
      if (params.nonEmpty) {
        " -p " + params.mkString(",")
      } else {
        ""
      }
    } + "\"").right.flatMap { res =>
      Logger.debug("Job " + hdfsResDir + " finished: " + res.exitCode.get)
      //Make the directory usable by others in the group
      client.exec("hadoop fs -chmod -R 775 " + hdfsResDir)
    }
  }

  private def mergeAndDl(directory: String, file: String, dlDst: String)(implicit client: SshClient) = {
    // Download results from HDFS to local on cluster
    client.exec("hadoop fs -getmerge " + directory + " " + file).right.flatMap { res =>
      Logger.debug("get " + file.substring(2) + " done " + res.exitCode.get)
      // Download results from cluster to server
      client.download(file.substring(2), dlDst)
    }
  }

  private def rmLocalCopies(folder: String) = {
    import scala.sys.process._
    Process("rm -R ./public/results/" + folder).run
  }

  private def sparkSubmitDisplayer(words: List[String], hdfsResDir: String, hash: String)
                                  (client: SshClient): Validated[CommandResult] = {
    client.exec("bash -c \"source .bashrc; spark-submit --class DisplayCommander --master yarn-cluster " +
      "--num-executors 25 SparkCommander-assembly-1.0.jar -h " + hash + " -w " + words.mkString(",") + "\"").right
      .flatMap({ res =>
      //      Logger.debug(res.stdErrAsString())
      Logger.debug("Job " + hdfsResDir + " finished: " + res.exitCode.get)
      client.exec("hadoop fs -chmod -R 775 " + hdfsResDir)
    })
  }
}