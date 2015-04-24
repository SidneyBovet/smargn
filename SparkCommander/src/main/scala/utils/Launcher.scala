package utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Formatting._
import utils.Grapher._


/**
 * Created by Joanna on 4/7/15.
 * Main launcher of the program
 */
object Launcher {
  private type Technique = (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[String]
  private val startYear = 1840
  private val endYear = 2000
  private val NB_RES = 10
  val NSW = "NOSIMILARWORDS"
  val NOTFOUND = "ERROR404"

  def runList(words: List[String], inputDir: String, outputFile: String, parameters: List[Double],
              similarityTechnique: Technique, spark: SparkContext,
              range: Range = startYear to endYear): Map[String, List[String]] = {
    val hdfs = new HDFSHandler(spark.hadoopConfiguration)

    // Create folder for result
    hdfs.createFolder(outputFile)

    // Getting results for all words
    val results = words.map(w =>
      w -> run(w, inputDir, outputFile, parameters, similarityTechnique, spark, hdfs, range)).toMap

    // Create or append to result file and
    // append results found to results.txt
    val similarWordsPath = new Path(outputFile + "/results.txt")
    hdfs.appendToFile(similarWordsPath)(results.map { case (w, res) => s"$w -> ${res.mkString(" ")}" }.toList)
    hdfs.close()
    results
  }

  def run(word: String, inputDir: String, outputFile: String, parameters: List[Double], similarityTechnique: Technique,
          spark: SparkContext, hdfs: HDFSHandler, range: Range = startYear to endYear): List[String] = {
    val data = spark.textFile(inputDir)

    //Formatting part
    val formattedData = dataFormatter(data).cache()
    // testedWords is the line with the words we look for and its occurrences
    val testedWords = searchWordFormatter(formattedData, List(word))


    if (testedWords.count == 0) {
      List(NOTFOUND)
    } else {
      val testedWord = testedWords.first()

      //apply the similarity technique
      val similarWords = similarityTechnique(formattedData, testedWord, parameters).collect().toList

      // Get similar words with its occurrences
      val similarWordOcc = formattedData.filter { case (w, occurrences) => similarWords.contains(w) }

      // Format for printing
      val formatter = formatTuple(range) _
      val toPrintRDD = similarWordOcc.take(NB_RES).flatMap(formatter)
      val toGraph = formatForDisplay(formatter(testedWord), toPrintRDD)

      // Print to projects/temporal-profiles/<depends on the query>/data.csv
      val dataCSVPath = new Path(outputFile + "/data.csv")

      // Get stream by creating file projects/temporal-profiles/<depends on the query>/data.csv or appending to it
      // Print to projects/temporal-profiles/<depends on the query>/data.csv
      hdfs.appendToFile(dataCSVPath)(toGraph)

      if (similarWords.length == 0) {
        List(NSW)
      } else {
        similarWords
      }
    }
  }
}
