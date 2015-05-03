package masters

import masters.Launcher._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Formatting._
import utils.Grapher._

/**
 * From Valentin with love on 03/05/15.
 */
object Displayer {
  def runList(words: Seq[String], inputDir: String, outputFile: String, spark: SparkContext,
              range: Range = startYear to endYear): Unit = {

    // Getting results for all words
    val data = spark.textFile(inputDir)
    val gData = words.map { w => run(w, data, spark, range) }.reduce(_ ++ _)
    // Write results to /projects/temporal-profile/results/<outputdir>/
    gData.saveAsTextFile(outputFile)
  }

  private def run(word: String, data: RDD[String], spark: SparkContext, range: Range): RDD[String] = {
    //Formatting part
    val formattedData = dataFormatter2(data).cache()
    // testedWords is the line with the words we look for and its occurrences
    val testedWords = searchWordFormatter(formattedData, List(word))

    if (testedWords.count == 0) {
      spark.emptyRDD[String]
    } else {
      // Format for printing commutative in order to be parallelizable!
      testedWords.flatMap(formatTuple(range))
    }
  }
}
