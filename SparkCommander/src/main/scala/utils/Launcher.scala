package utils

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Formatting._

/**
 * Created by Joanna on 4/7/15.
 * Main launcher of the program
 */
object Launcher {
  private type Technique = (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[String]
  private val startYear = 1840
  private val endYear = 2000
  private val NB_RES = 10

  def runList(words: List[String], inputDir: String, outputFile: String, parameters: List[Double],
              similarityTechnique: Technique, spark: SparkContext,
              range: Range = startYear to endYear): Map[String, List[String]] = {
    words.map(w => w -> run(w, inputDir, outputFile, parameters, similarityTechnique, spark, range)).toMap
  }

  def run(word: String, inputDir: String, outputFile: String, parameters: List[Double], similarityTechnique: Technique,
          spark: SparkContext, range: Range = startYear to endYear): List[String] = {

    val data = spark.textFile(inputDir)

    val target = new File(outputFile)
    if (target.exists()) {
      deleteFolder(target)
    }

    //Formatting part
    val formattedData = dataFormatter(data)
    val testedWords = searchWordFormatter(formattedData, List(word)).cache()

    if (testedWords.count == 0) {
      List("ERROR404")
    } else {
      val testedWord = testedWords.first()

      //apply the similarity technique
      val similarWords = similarityTechnique(formattedData, testedWord, parameters).cache()

      //Take 10 words only
      val similarWordOcc = testedWords
        .filter { case (w1, occurrences) => similarWords.filter(w2 => w1 == w2).count() != 0
      }

      val toPrintRDD = similarWordOcc.flatMap(Grapher.formatForDisplay(range))

      toPrintRDD.saveAsTextFile(outputFile)

      //Graph displaying part
      val similarities: RDD[(String, Array[Double])] =
        searchWordFormatter(formattedData, similarWords.collect().toList)

      similarities.map(_._1).collect().toList
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
