package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Formatting._
import utils.Grapher._


/**
 * Created by Joanna on 4/7/15.
 * Main launcher of the program
 */
object Launcher {
  type Technique = (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[String]
  private val startYear = 1840
  private val endYear = 1998
  private val NB_RES = 10
  val NSW = "NOSIMILARWORDS"
  val NOTFOUND = "ERROR404"

  def runList(words: Seq[String], inputDir: String, baseProfileFile: String, outputFile: String,
              parameters: List[Double], similarityTechnique: Technique, spark: SparkContext,
              range: Range = startYear to endYear): Unit = {

    // Getting results for all words
    val data = spark.textFile(inputDir)
    val baseProfile = spark.textFile(baseProfileFile).take(1)(0).split(" ").map(_.toInt)
    val (res, gData) = words.map { w =>
      run(w, data, baseProfile, outputFile, parameters, similarityTechnique, spark, range)
    }.unzip
    val (results, graphData) = (res.reduce(_ ++ _), gData.reduce(_ ++ _))
    // Write results to /projects/temporal-profile/results/<outputdir>/results/
    results.saveAsTextFile(outputFile + "results/")
    // Write results to /projects/temporal-profile/results/<outputdir>/data/
    graphData.saveAsTextFile(outputFile + "data/")
  }

  private def run(word: String, data: RDD[String], baseProfile: Array[Int], outputFile: String,
                  parameters: List[Double], similarityTechnique: Technique, spark: SparkContext,
                  range: Range = startYear to endYear): (RDD[String], RDD[String]) = {
    val emptyRDD: RDD[String] = spark.emptyRDD[String]

    //Formatting part
    val formattedData = dataFormatter(data, baseProfile)
    // testedWords is the line with the words we look for and its occurrences
    val testedWords = searchWordFormatter(formattedData, List(word))

    if (testedWords.count == 0) {
      (spark.parallelize(Seq(word + " -> " + NOTFOUND)), emptyRDD)
    } else {
      val testedWord = testedWords.first()

      //apply the similarity technique
      val similarWords = similarityTechnique(formattedData, testedWord, parameters)

      if (similarWords.count() == 0) {
        (spark.parallelize(Seq(word + " -> " + NSW)), emptyRDD)
      } else {
        // Get similar words with their occurrences
        // RDD[T].contains(T) does not exist in Spark 1.2.x and impossible to nest RDD queries so we join them
        // Another (better) choice would be to send back the similar words with their occurrences
        // instead of doing the job twice
        import org.apache.spark.SparkContext._

        val formattedDataKey = formattedData.groupBy(_._1)
        val similarWordsKey = similarWords.groupBy(x => x)
        val similarWordOcc = formattedDataKey.join(similarWordsKey).flatMap { case (k, (wo, w)) => wo
        }

        // Format for printing
        val formatter = formatTuple(range) _
        // We use reduce because it is parallelizable.
        // The function reduce takes a binary operator which has to be commutative in order to be parallelizable!
        // in this case, we do not care about the order of the words
        (spark.parallelize(Seq(word + " -> " + similarWords.reduce(_ + " " + _))), testedWords.flatMap(formatter) ++
          spark.parallelize(similarWordOcc.flatMap(formatter).take(NB_RES * range.size)))
      }
    }
  }
}