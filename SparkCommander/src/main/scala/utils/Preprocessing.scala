package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import techniques.{DynamicTimeWrapping, PeakComparison, Divergence, NaiveComparisons}
import utils.Launcher._


import scala.collection.immutable.Range

/**
 * Created by fabien on 5/17/15.
 */
object Preprocessing {

  // contains test cases of the form: wordToTest similar1,similar2,... nonSimilar1,nonSimilar2,...
  val inputParams = "hdfs:///projects/temporal-profiles/preprocessing/params.txt"

  // Parses the boundaries for each techniques
  def parseParams(spark: SparkContext): Array[(String, List[Double])] = {
    val params = spark.textFile(inputParams)

    params.map(line => {
      val lineSplit = line.split("\\s")


      (lineSplit.head, lineSplit.tail.toList.map(x => x.toDouble))

    }).collect()
  }

  private def createOutput(mode: String, words: Seq[String], technique: String, params: Seq[Double]): String = {
    s"hdfs:///projects/temporal-profiles/results/${
      if (mode != null) s"${mode}_"
      else ""
    }${words.mkString("-")}${
      if (params.nonEmpty) {
        s"_${technique.toLowerCase}_${
          params.mkString("-")
        }"
      } else {
        ""
      }
    }/"

  }


  def preprocess(spark: SparkContext, inputDir: String, baseProfileFile: String, outputFile: String): Unit = {
    val params = parseParams(spark)

    val hdfs = new HDFSHandler(spark.hadoopConfiguration)

    val data = spark.textFile(inputDir)



    //val words = Array(data.collect().head)
    //val words = data.collect().dropRight(10)
    val words = data.collect().dropRight(100)


    words.foreach(word => {
      params.foreach(tech => {
        runList(List(word), inputDir, baseProfileFile, createOutput(null, List(word), tech._1, tech._2),
          tech._2, tech._1, spark)
      })
    })


  }

}
