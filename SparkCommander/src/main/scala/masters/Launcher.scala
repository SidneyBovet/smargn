package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Formatting._
import utils.Grapher._
import techniques.{DynamicTimeWrapping, Divergence, NaiveComparisons, PeakComparison}

import org.apache.hadoop.fs.Path
import utils.TestCases._
import utils.Preprocessing._

/*
 * Contributors:
 *  - Valentin Rutz: run, runList
 */

/**
 * Created by Joanna on 4/7/15, modified by almost all the team.
 * Main launcher of the program
 */
object Launcher {
  type Technique = (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[String]

  //type of techniques that compare the words given in the list
  type CompareTechnique = ((String, Array[Double]), (String, Array[Double]), List[Double]) => List[(String, String,
    Double)]

  type Metric = (Array[Double], Array[Double], List[Double]) => Double
  val startYear = 1840
  val endYear = 1998
  val NSW = "NOSIMILARWORDS"
  val NOTFOUND = "ERROR404"
  val SYNONYMS_INPUT = "hdfs:///projects/temporal-profiles/data-generation/synonyms"


  def tech(technique: String): Technique = technique match {
    // Add your technique methods here. All lowercase for the name pliz
    case "naivedifference" => NaiveComparisons.naiveDifferenceTopKScalingAverage
    case "naivedivision" => NaiveComparisons.naiveDivisionTopKScalingAverage
    case "naivedifferencesquared" => NaiveComparisons.naiveDifferenceSquaredTopKScalingAverage
    case "naivedivisionvar" => NaiveComparisons.naiveDivisionVarianceTopKScalingAverage
    case "inverse" => NaiveComparisons.naiveInverseDifference
    case "shift" => NaiveComparisons.naiveDifferenceScalingAverageWithShifting
    case "divergence" => Divergence.naiveDifferenceDivergence
    case "smarterdivergence" => SubTechniques.smarterDivergence
    case "peaks" => PeakComparison.peakComparisonWithMeanDerivative
    case "dtw" => DynamicTimeWrapping.dtwComparison
    case "dtwtopk" => DynamicTimeWrapping.dtwSimpleTopK
    case "dtwscaleavgtopk" => DynamicTimeWrapping.dtwComparisonScaleAvgTopK
    case "dtwscalemaxtopk" => DynamicTimeWrapping.dtwComparisonScaleMaxTopK
    case "peakstopk" => PeakComparison.peaksTopK
  }

  def filterBySynonyms(data: RDD[(String, Array[Double])], similarityTechnique: String,
                       word: String, spark: SparkContext): RDD[(String, Array[Double])] = {

    if (similarityTechnique == "smarterdivergence") {
      // Load the list of synonyms
      val synonymsCollection = spark.textFile(SYNONYMS_INPUT)
      // Format list of synonyms (word, Array(synonyms)) and get only the interesting word
      val synonyms = synonymsCollection.map(line => {
        val splittedLine = line.split(",")
        (splittedLine.head, splittedLine)
      }).filter(_._1 == word).map(_._2).collect()
      if (synonyms.length <= 0) {
        spark.emptyRDD[(String, Array[Double])]
      } else {
        data.filter(word => synonyms.head.contains(word._1));
      }
    } else {
      data
    }
  }

  /**
   * Runs the technique on each words, gathers the results and writes to HDFS
   * @param words the words on which we apply the technique
   * @param inputDir path to input data
   * @param baseProfileFile base profile of temporal profiles for scaling
   * @param outputFile output directory for results
   * @param parameters parameters for the technique
   * @param similarityTechnique technique to apply
   * @param spark Spark context
   * @param range range of years
   */
  def runList(words: Seq[String], inputDir: String, baseProfileFile: String, outputFile: String,
              parameters: List[Double], similarityTechnique: String, spark: SparkContext,
              range: Range = startYear to endYear): Unit = {

    // Getting results for all words
    val data = spark.textFile(inputDir)
    val baseProfile = spark.textFile(baseProfileFile).take(1)(0).split(" ").map(_.toInt)

    //Formatting part
    val formattedData = dataFormatter(data, baseProfile)

    val (res, gData) = words.map { w =>
      val filteredData = filterBySynonyms(formattedData, similarityTechnique, w, spark)
      run(w, filteredData, outputFile, parameters, tech(similarityTechnique), spark, range)
    }.unzip
    val (results, graphData) = (res.reduce(_ ++ _), gData.reduce(_ ++ _))
    // Write results to /projects/temporal-profile/results/<outputdir>/results/
    results.saveAsTextFile(outputFile + "results/")
    // Write results to /projects/temporal-profile/results/<outputdir>/data/
    graphData.saveAsTextFile(outputFile + "data/")
  }

  /**
   *
   * @param words the words to be compared
   * @param inputDir the HDFS directory containing all the temporal profiles
   * @param baseProfileFile the file containing the base profile (all words written per year)
   * @param outputFile where the result should be stored (HDFS)
   * @param parameters parameters for the technique, size may vary according to technique
   * @param similarityTechnique the technique to be used for the comparison
   * @param spark the [[SparkContext]] to be used for this task
   * @param range (optional) the year range on which to perform the search
   */
  def runCompare(words: Seq[String], inputDir: String, baseProfileFile: String, outputFile: String,
                 parameters: List[Double], similarityTechnique: String, spark: SparkContext,
                 range: Range = startYear to endYear): Unit = {

    // Getting results for all words
    val data = spark.textFile(inputDir)
    val baseProfile = spark.textFile(baseProfileFile).take(1)(0).split(" ").map(_.toInt)

    //Formatting part
    val formattedData = dataFormatter(data, baseProfile)
    // testedWords is the line with the words we look for and its occurrences
    //val profiles = words.map(x => searchWordFormatter(formattedData, List(x)))
    val profiles = searchWordFormatter(formattedData, words.toList)
    val peaks = PeakComparison.getPeaks(profiles, parameters)
    peaks.saveAsTextFile(outputFile + "peaks/")

  }


  /**
   * Runs the tuning of the metrics
   * @param sc the [[SparkContext]] to be used for this task
   * @param inputDir the HDFS directory containing all the temporal profiles
   * @param baseProfileFile the file containing the base profile (all words written per year)
   */
  def runParamsFinding(sc: SparkContext, inputDir: String, baseProfileFile: String): Unit = {
    val outputDir = "hdfs:///projects/temporal-profiles/results/testCases"
    val hdfs = new HDFSHandler(sc.hadoopConfiguration)

    // Create folder for result
    hdfs.createFolder(outputDir)


    val data = sc.textFile(inputDir)
    val baseProfile = sc.textFile(baseProfileFile).take(1)(0).split(" ").map(_.toInt)
    val formattedData = dataFormatter(data, baseProfile)


    val res: Array[Array[(String, Double, List[Double], List[String])]] = runTestsAll(sc, formattedData)


    val resPath = new Path(outputDir + "/results.txt")
    //val logPath = new Path(outputDir + "/logs.txt")


    hdfs.appendToFile(resPath)(res.flatMap(x => x.map { case (a, b, c, d) => s"$a, $b, ${c.mkString(" ")}" }).toList)

    //hdfs.appendToFile(logPath)(res.flatMap(x => x.flatMap(y => y._4)).toList.reverse)
  }

  def runPreprocessing(sc: SparkContext, inputDir: String, baseProfileFile: String, outputFile: String): Unit = {

    /*val outputDir = "hdfs:///projects/temporal-profiles/results/preProcessing"
    val hdfs = new HDFSHandler(sc.hadoopConfiguration)

    // Create folder for result
    hdfs.createFolder(outputDir)

    //139006 words
    val data = sc.textFile(inputDir)

    val testPath = new Path(outputDir + "/test.txt")

    hdfs.appendToFile(testPath)(List(data.count()+" words"))
    hdfs.close()*/

    preprocess(sc, inputDir, baseProfileFile, outputFile)


  }

  /**
   * Runs the technique on the word
   * @param word the words on which we apply the technique
   * @param data input data
   * @param outputFile output directory for results
   * @param parameters parameters for the technique
   * @param similarityTechnique technique to apply
   * @param spark Spark context
   * @param range range of years
   */
  private def run(word: String, data: RDD[(String, Array[Double])], outputFile: String,
                  parameters: List[Double], similarityTechnique: Technique, spark: SparkContext,
                  range: Range = startYear to endYear): (RDD[String], RDD[String]) = {
    val emptyRDD: RDD[String] = spark.emptyRDD[String]

    // testedWords is the line with the words we look for and its occurrences
    val testedWords = searchWordFormatter(data, List(word))

    if (testedWords.count == 0) {
      (spark.parallelize(Seq(word + " -> " + NOTFOUND)), emptyRDD)
    } else {
      val testedWord = testedWords.first()

      //apply the similarity technique
      val similarWords = similarityTechnique(data, testedWord, parameters)

      if (similarWords.count() == 0) {
        (spark.parallelize(Seq(word + " -> " + NSW)), emptyRDD)
      } else {
        // Get similar words with their occurrences
        // RDD[T].contains(T) does not exist in Spark 1.2.x and impossible to nest RDD queries so we join them
        // Another (better) choice would be to send back the similar words with their occurrences
        // instead of doing the job twice
        import org.apache.spark.SparkContext._

        val formattedDataKey = data.groupBy(_._1)
        val similarWordsKey = similarWords.groupBy(x => x)
        val similarWordOcc = formattedDataKey.join(similarWordsKey).flatMap { case (k, (wo, w)) => wo }

        // Format for printing
        val formatter = formatTuple(range) _
        // We use reduce because it is parallelizable.
        // The function reduce takes a binary operator which has to be commutative in order to be parallelizable!
        // in this case, we do not care about the order of the words
        (spark.parallelize(Seq(word + " -> " + similarWords.reduce(_ + " " + _))), testedWords.flatMap(formatter) ++
          similarWordOcc.flatMap(formatter))
      }
    }
  }
}
