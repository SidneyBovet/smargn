package utils

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Launcher.Technique

import techniques._

/**
 * Created by fabien and mathieu on 4/28/15.
 */
object TestCases {

  val logger = Logger.getLogger(TestCases.getClass.getName)

  // TODO: use the files as input test cases and parameters bounds
  // contains test cases of the form: wordToTest similar1,similar2,... nonSimilar1,nonSimilar2,...
  val inputCases = "hdfs:///projects/temporal-profiles/Tests/testCases"

  // contains techniques and parameters bound: name lowerBound1,upperBound1,nbSteps1 lowerBound2,upperBound2,nbSteps2 for each parameter
  val inputParams = "hdfs:///projects/temporal-profiles/Tests/params"


  /**
   * This function was used for debugging because we didn't manage to log stuff inside of the map of a RDD
   * @param log The list containing previous log entries
   * @param msg The message to add to the log
   * @return The Updated log
   */
  def printLog(log: List[String], msg: String): List[String] = msg::log


  /**
   * Read and parse the test cases from an input file
   * @param spark The spark context
   * @return An array containing the test cases
   */
  def parseTestCases(spark: SparkContext): Array[(String, List[String], List[String])] = {
    val testCases = spark.textFile(inputCases)

    testCases.map(line => {
      val tmp = line.split("\\s")
      (tmp(0), tmp(1).split(",").toList, tmp(2).split(",").toList)
    }).collect()
  }

  /**
   * Read and parse the technique that we will use for the tuning
   * @param spark The spark context
   * @return An array with the techniques and their parameters
   */
  def parseTechniques(spark: SparkContext): Array[(Technique, String,  List[(Double, Double, Double)])] = {
    val params = spark.textFile(inputParams)

    params.map(line => {
      val lineSplit = line.split("\\s")
      (lineSplit(0), lineSplit.drop(1).map(s => {
        val tuple = s.split(",")
        (tuple(0), tuple(1), tuple(2))
      }).toList)
    }).map(x => (getTechnique(x._1), x._1, x._2.map(y => (y._1.toDouble, y._2.toDouble, y._3.toDouble)))).collect()
  }

  /**
   * Counts how many words from the given test case appear in the result of a technique
   * @param result The result of a technique
   * @param wordList A list of word from the test cases
   * @return How many words are contained in both lists
   */
  def count(result: RDD[(String)], wordList: List[String]): Int = {

    result.mapPartitions(it => {
      var value=0
      while(it.hasNext) {
        if(wordList.contains(it.next())) {
          value += 1
        }
      }
      (value::Nil).iterator
    },true).collect().sum

  }

  /**
   * runs a technique with the given parameters
   * @param data The data set containing all the words
   * @param testedWord The word to test
   * @param similarWords The list of similar words
   * @param differentWords The list of different words
   * @param parameters The parameters for the technique
   * @param similarityTechnique The technique we want to use
   * @return A double between 0 and 1 that tells how well the technique matches the test caes
   */
  def test(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), similarWords: List[String],
           differentWords: List[String], parameters: List[Double], similarityTechnique: Technique): (Double, List[String]) = {
    //var log = List[String]()

    //log = printLog(log,"Trying with parameters "+parameters+".")
    val result: RDD[(String)] = similarityTechnique(data, testedWord, parameters)
    //log = printLog(log,result.count() + " Matching words:")

    //result.collect().foreach(x => log=printLog(log,"word: "+x))

    //log = printLog(log, similarWords.size + " similar words: ")

    //similarWords.foreach(x => log=printLog(log,"word: "+x))

    val simWords = count(result, similarWords)
    val diffWords = count(result, differentWords)

    //log = printLog(log, simWords + " match(es)")

    val simRatio = simWords.toDouble / similarWords.size.toDouble
    val diffRatio = diffWords.toDouble / differentWords.size.toDouble

    (((simRatio + (1 - diffRatio)) / 2),Nil)
    //(simRatio,Nil)
  }


  /**
   * Recursive function to iterate over the parameters of a metric. This function shouldn't be called directly,
   * one should use testParameters instead.
   * @param data The data set containing all the words
   * @param testedWord The word to test
   * @param similarWords The list of similar words
   * @param differentWords The list of different words
   * @param params The list of fixed parameters (that were chosen by the previous iteration of the function)
   * @param bounds The bounds for the remaining parameters
   * @param similarityTechnique The technique we want to use
   * @return The parameters that outputs the best value
   */
  def getBestParams(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), similarWords: List[String],
                    differentWords: List[String], params: List[Double], bounds: List[(Double, Double, Double)],
                    similarityTechnique: Technique): (Double, List[Double], List[String]) = {
    bounds match {
      case x :: xs => {
        val step = x._3

        //if step == 0 we do no optimization for this parameter
        if (step == 0.0) {
          getBestParams(data, testedWord, similarWords, differentWords, params ++ (x._1 :: Nil), xs,
            similarityTechnique)
        }
        else {
          var best = (0.0, List[Double](),List[String]())

          for (y <- Range.Double.inclusive(x._1, x._2, step)) {
          //for (y <- x._1 to(x._2, (x._2 - x._1) / step)) {
            val res = getBestParams(data, testedWord, similarWords, differentWords, params ++ (y :: Nil), xs,
              similarityTechnique)
            if (res._1 > best._1) {
              best = res
            }
          }
          best
        }

      }
      case Nil => {
        val testProut = test(data, testedWord, similarWords, differentWords, params, similarityTechnique)
        (testProut._1, params, testProut._2)
      }
    }
  }

  /**
   * Iterate the technique over all the possible parameters and returns a list containing the best parameters.
   * This is a wrapper for the getBestParams function.
   * @param data The data set containing all the words
   * @param testedWord The word to test
   * @param similarWords The list of similar words
   * @param differentWords The list of different words
   * @param bounds The bounds for the parameters
   * @param similarityTechnique The technique we want to use
   * @return The parameters that outputs the best value
   */
  def testParameters(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                     similarWords: List[String], differentWords: List[String], bounds: List[(Double, Double, Double)],
                     similarityTechnique: Technique): (Double, List[Double], List[String]) = {
    getBestParams(data, testedWord, similarWords, differentWords, Nil, bounds, similarityTechnique)
  }

  /**
   * Maps a technique name with the actual technique
   * @param name Name of the technique
   * @return The technique corresponding to this name
   */
  def getTechnique(name: String): Technique = {
    name.toLowerCase match {
          // Add your technique methods here. All lowercase for the name pliz
          case "naivedifference" => NaiveComparisons.naiveDifferenceTopKScalingAverage
          case "naivedivision" => NaiveComparisons.naiveDivisionTopKScalingAverage
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
          case _ => NaiveComparisons.naiveDifferenceTopKScalingAverage
        }
  }


  /**
   * Read test cases and technique parameters from hdfs:///projects/temporal-profiles/Tests/testCases
   * and hdfs:///projects/temporal-profiles/Tests/params respectively, and tries to find optimal
   * parameters for each technique and each test case.
   * @param spark SparkContext used to read the config files
   * @param data collection of word, frequency to tuple to look into
   * @return optimal parameters for each technique and each test case
   */
  def runTestsAll(spark: SparkContext,
                  data: RDD[(String, Array[Double])]): Array[Array[(String, Double, List[Double], List[String])]] = {

    val techniques = parseTechniques(spark)
    val testCases = parseTestCases(spark)


    techniques.map(x => runTests(spark, data, x._1, x._2, x._3, testCases))

  }

  /**
   * Same as runTestsAll, but for a single technique.
   * @param spark SparkContext used to read the config files
   * @param data collection of word, frequency to tuple to look into
   * @param technique the actual technique
   * @param techniqueName the name of the technique (used in the result file)
   * @param techniqueParams the bounds of the parameters
   * @param testCases the test case (1 word to test, 1 list of similar words and 1 list of different words)
   * @return optimal parameters for each test case
   */
  def runTests(spark: SparkContext, data: RDD[(String, Array[Double])], technique: Technique,
               techniqueName: String, techniqueParams: (List[(Double, Double, Double)]),
               testCases: Array[(String, List[String], List[String])] = null): Array[(String, Double, List[Double], List[String])] = {


    val test = testCases match {
      case null => parseTestCases(spark)
      case _ => testCases
    }

    test.map(t => {
      val testName = techniqueName+"_"+t._1

      logger.debug("Starting optimization for \""+techniqueName+"\".")

      val result = testParameters(data, data.filter(x => x._1.equals(t._1)).first(), t._2, t._3, techniqueParams,
        technique)


      (testName, result._1, result._2, result._3)
    })
  }
}
