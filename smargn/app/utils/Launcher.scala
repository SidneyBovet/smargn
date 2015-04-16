package utils

import java.io.{File, PrintWriter}

import controllers.Spark
import org.apache.spark.rdd.RDD
import play.Logger
import utils.Formatting._

/**
 * Created by Joanna on 4/7/15.
 * Main launcher of the program
 */
object Launcher {

  def runList(words: List[String], inputDir: String, outputFile: String, parameters: List[Double],
              similarityTechnique: (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[
                (String)],
              range: Range = 2000 to 2004): Map[String, List[String]] = {
    words.map(w => w -> run(w, inputDir, outputFile, parameters, similarityTechnique, range)).toMap
  }

  def run(word: String, inputDir: String, outputFile: String, parameters: List[Double],
          similarityTechnique: (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[(String)],
          range: Range = 2000 to 2004): List[String] = {
    val spark = Spark.ctx
    Logger.info("Searching for word: " + word)

    val data = spark.textFile(inputDir)

    val target = new File(outputFile)
    if (target.exists()) {
      Logger.info("Deleting previous output folder")
      deleteFolder(target)
    }

    //Formatting part
    val formattedData = dataFormatter(data)
    val testedWords = searchWordFormatter(formattedData, List(word))

    if (testedWords.count == 0) {
      Logger.debug(word + " was not found previously in the data")
      List("ERROR404")
    } else {
      val testedWord = testedWords.first()

      //apply the similarity techique
      val similarWords = similarityTechnique(formattedData, testedWord, parameters)

      similarWords.saveAsTextFile(outputFile)

      //Graph displaying part
      val similaritiesLocal: List[(String, Array[Double])] =
        searchWordFormatter(formattedData, similarWords.collect().toList).collect().toList

      val toPrint = Grapher.formatForDisplay(range, testedWord, similaritiesLocal)

      printToFile(new File(outputFile + "data.csv")) { p => toPrint.foreach(p.println) }

      Logger.info("Found " + similarWords.count() + " similar words")
      similaritiesLocal.map(_._1)
    }
  }

  def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles
    files.foreach(f => {
      if (f.isDirectory) {
        deleteFolder(f)
      } else {
        f.delete
      }
    })
    folder.delete
  }

  def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

}
