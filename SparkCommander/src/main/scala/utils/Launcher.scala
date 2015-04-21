package utils

import java.io.{PrintWriter, File}

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

  def runList(words: List[String], inputDir: String, outputFile: String, parameters: List[Double],
              similarityTechnique: Technique, spark: SparkContext,
              range: Range = startYear to endYear): Map[String, List[String]] = {
    words.map(w => w -> run(w, inputDir, outputFile, parameters, similarityTechnique, spark, range)).toMap
  }

  def run(word: String, inputDir: String, outputFile: String, parameters: List[Double], similarityTechnique: Technique,
          spark: SparkContext, range: Range = startYear to endYear): List[String] = {
    val hdfs = new HDFSHandler(spark.hadoopConfiguration)
    val data = spark.textFile(inputDir)


    //Formatting part
    val formattedData = dataFormatter(data).cache()
    // testedWords is the line with the words we look for and its occurrences
    val testedWords = searchWordFormatter(formattedData, List(word))

    if (testedWords.count == 0) {
      List("ERROR404")
    } else {
      hdfs.createFolder(outputFile)
      val testedWord = testedWords.first()

      //apply the similarity technique
      val similarWords = similarityTechnique(formattedData, testedWord, parameters).cache()

      // Get similar words with its occurrences
      val similarWordOcc = formattedData
        .filter { case (w1, occurrences) => similarWords.filter(w2 => w1 == w2).count() != 0
      }

      // Format for printing
      val formatter = formatTuple(range)
      val toPrintRDD = similarWordOcc.flatMap(formatter)
      val toGraph = formatForDisplay(formatter(testedWord), toPrintRDD.top(NB_RES))

      // Print to projects/temporal-profiles/<depends on the query>/data.csv
      val data_csv = hdfs.createFile(new Path(outputFile + "/data.csv"))
      data_csv(toGraph.mkString("\n"))
      printToFile(new File(outputFile + "/data.csv")) { p => toGraph.foreach(p.println) }

      val result = similarWords.collect().toList
      if (result.length == 0) {
        List("NOSIMILARWORDS")
      } else {
        result
      }
    }
  }

  private def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}
