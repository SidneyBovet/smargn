package masters

import org.apache.spark.SparkContext
import utils.Formatting._
import utils.Grapher._

/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * From Valentin with love on 03/05/15.
 */
object Displayer {
  private val startYear = 1840
  private val endYear = 1998

  def runList(words: Seq[String], baseProfileFile: String, inputDir: String, outputFile: String, spark: SparkContext,
              range: Range = startYear to endYear): Unit = {
    // Getting results for all words
    val baseProfile = spark.textFile(baseProfileFile).take(1)(0).split(" ").map(_.toInt)
    val data = dataFormatter(spark.textFile(inputDir), baseProfile)
    val wordsOcc = data.filter { case (w, occ) => words.contains(w) }.flatMap(formatTuple(range))
    wordsOcc.saveAsTextFile(outputFile)
  }
}
