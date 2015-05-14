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

  // Parses the test cases in the file inputCases
  def parseTestCases(spark: SparkContext): Array[(String, List[String], List[String])] = {
    val testCases = spark.textFile(inputCases)

    testCases.map(line => {
      val tmp = line.split("\\s")
      (tmp(0), tmp(1).split("\\s").toList, tmp(2).split("\\s").toList)
    }).collect()
  }

  // Parses the boundaries for each techniques
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

  def count(result: RDD[(String)], wordList: List[String]): Int = {
    var value = 0
    wordList.foreach(word => result.foreach(x => if (x == word) {
      value += 1
    }))
    value
  }

  // Evaluates a technique with some fixed parameters
  def test(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), similarWords: List[String],
           differentWords: List[String], parameters: List[Double], similarityTechnique: Technique): Double = {

    logger.debug("Trying with parameters "+parameters+".")
    val result: RDD[(String)] = similarityTechnique(data, testedWord, parameters)

    val simWords = count(result, similarWords)
    val diffWords = count(result, differentWords)
    val simRatio = simWords.toDouble / similarWords.size.toDouble
    val diffRatio = diffWords.toDouble / differentWords.size.toDouble

    //(simRatio + (1 - diffRatio)) / 2
    simRatio
  }

  // Iterates over all the possible parameters and output the best combination
  def getBestParams(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), similarWords: List[String],
                    differentWords: List[String], params: List[Double], bounds: List[(Double, Double, Double)],
                    similarityTechnique: Technique): (Double, List[Double]) = {
    bounds match {
      case x :: xs => {
        val step = x._3

        //if step == 0 we do no optimization for this parameter
        if (step == 0.0) {
          getBestParams(data, testedWord, similarWords, differentWords, params ++ (x._1 :: Nil), xs,
            similarityTechnique)
        }
        else {
          var best = (0.0, List[Double]())

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
      case Nil => (test(data, testedWord, similarWords, differentWords, params, similarityTechnique), params)
    }
  }

  def testParameters(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                     similarWords: List[String], differentWords: List[String], bounds: List[(Double, Double, Double)],
                     similarityTechnique: Technique): (Double, List[Double]) = {
    getBestParams(data, testedWord, similarWords, differentWords, Nil, bounds, similarityTechnique)
  }

  // Gets the technique associated with that name
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
   * @return optimal parameters for each technique and each test cases
   */
  def runTestsAll(spark: SparkContext,
                  data: RDD[(String, Array[Double])]): Array[Array[(String, Double, List[Double])]] = {

    val techniques = parseTechniques(spark)
    val testCases = parseTestCases(spark)


    techniques.map(x => runTests(spark, data, x._1, x._2, x._3, testCases))

  }

  def runTests(spark: SparkContext, data: RDD[(String, Array[Double])], technique: Technique,
               techniqueName: String, techniqueParams: (List[(Double, Double, Double)]),
               testCases: Array[(String, List[String], List[String])] = null): Array[(String, Double, List[Double])] = {


    val test = testCases match {
      case null => parseTestCases(spark)
      case _ => testCases
    }

    test.map(t => {
      val testName = techniqueName+"_"+t._1

      logger.debug("Starting optimization for \""+techniqueName+"\".")

      val result = testParameters(data, data.filter(x => x._1.equals(t._1)).first(), t._2, t._3, techniqueParams,
        technique)


      (testName, result._1, result._2)
    })
  }
}
