package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Launcher.Technique

import techniques.NaiveComparisons._
import techniques.NaiveInverseComparisons._
import techniques.NaiveShiftComparison._

/**
 * Created by fabien and mathieu on 4/28/15.
 */
object TestCases {
  // TODO: use the files as input test cases and parameters bounds
  // contains test cases of the form: wordToTest similar1,similar2,... nonSimilar1,nonSimilar2,...
  val inputCases = "hdfs:///projects/temporal-profiles/Tests/testCases"

  // contains techniques and parameters bound: name lowerBound1,upperBound1 lowerBound2,upperBound2 for each parameter
  val inputParams = "hdfs:///projects/temporal-profiles/Tests/params"

  // Parses the test cases in the file inputCases
  def parseTestCases(spark: SparkContext): List[(String, List[String], List[String])] = {
    //val testCases = spark.textFile(inputCases)
    val testCases = List[String]() //TODO read data as a List


    testCases.map(line => {
      val tmp = line.split("\\s")
      (tmp(0), tmp(1).split("\\s").toList, tmp(2).split("\\s").toList)
    })
  }

  // Parses the boundaries for each techniques
  def parseTechniques(spark: SparkContext): List[(Technique, List[(Double, Double)])] = {
    //val params = spark.textFile(inputParams)
    val params = List[String]() //TODO read data as a List


    params.map(line => {
      val lineSplit = line.split("\\s")
      (lineSplit(0), lineSplit.drop(1).map(s => {
        val tuple = s.split(",")
        (tuple(0), tuple(1))
      }).toList)
    }).map(x => (getTechnique(x._1),x._2.map(y => (y._1.toDouble,y._2.toDouble))))

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

    val result: RDD[(String)] = similarityTechnique(data, testedWord, parameters)

    val simWords = count(result, similarWords)
    val diffWords = count(result, differentWords)
    val simRatio = simWords.toDouble / similarWords.size.toDouble
    val diffRatio = diffWords.toDouble / similarWords.size.toDouble

    (simRatio + (1 - diffRatio)) / 2
  }

  // Iterates over all the possible parameters and output the best combination
  def getBestParams(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), similarWords: List[String],
                    differentWords: List[String], params: List[Double], bounds: List[(Double, Double)], iterations: Int,
                    similarityTechnique: Technique): (Double, List[Double]) = {
    bounds match {
      case x :: xs => {
        var best = (0.0, List[Double]())
        for (y <- x._1 to(x._2, (x._2 - x._1) / iterations.toDouble)) {
          val res = getBestParams(data, testedWord, similarWords, differentWords, params ++ (y :: Nil), xs, iterations,
            similarityTechnique)
          if (res._1 > best._1) {
            best = res
          }
        }
        best
      }
      case Nil => (test(data, testedWord, similarWords, differentWords, params, similarityTechnique), params)
    }
  }

  def testParameters(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                     similarWords: List[String], differentWords: List[String], bounds: List[(Double, Double)],
                     iterations: Int, similarityTechnique: Technique): List[Double] = {
    getBestParams(data, testedWord, similarWords, differentWords, Nil, bounds, iterations, similarityTechnique)._2
  }

  // TODO: Add the remaining techniques
  // Gets the technique associated with that name
  def getTechnique(name: String): Technique = {
    name.toLowerCase match {
      case "naiveinverse" => naiveInverseDifference
      case "naivedifferenceshift" => naiveDifferenceShift
      case _ => naiveDifference
    }
  }

  def runTestsAll(spark: SparkContext, data: RDD[(String, Array[Double])]): Unit = {
    val techniques = parseTechniques(spark)
    val testCases = parseTestCases(spark)
    val result = techniques.map(x => runTests(spark, data, x._1, x._2, testCases))
  }


  def runTests(spark: SparkContext, data: RDD[(String, Array[Double])], technique: Technique,
               techniqueParams: (List[(Double, Double)]), testCases: List[(String, List[String], List[String])] = null):
               List[(String, Double, List[Double])] = {

    var test: List[(String, List[String], List[String])] = null
    if (testCases == null) {
      test = parseTestCases(spark)
    }
    else {
      test = testCases
    }



    test.map(t =>
      (t._1, 0.0, testParameters(data, data.filter(x => x._1.equals(t._1)).first(), t._2, t._3, techniqueParams, 10, technique))
    )

  }
}
