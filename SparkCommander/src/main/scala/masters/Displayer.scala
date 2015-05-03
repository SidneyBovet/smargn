package masters

import masters.Launcher._
import org.apache.spark.SparkContext
import utils.Formatting._
import utils.Grapher._

/**
 * From Valentin with love on 03/05/15.
 */
object Displayer {
  def runList(words: Seq[String], inputDir: String, outputFile: String, spark: SparkContext,
              range: Range = startYear to endYear): Unit = {
    // Getting results for all words
    val data = dataFormatter2(spark.textFile(inputDir))
    val wordsOcc = data.filter { case (w, occ) => words.contains(w) }.flatMap(formatTuple(range))
    wordsOcc.saveAsTextFile(outputFile)
  }
}
