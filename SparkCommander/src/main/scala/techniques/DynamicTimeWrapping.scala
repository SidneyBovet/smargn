package techniques


import org.apache.spark.rdd.RDD
import utils.Scaling._
import utils.ComputationUtilities._
import utils.TopK._

/**
 * Created by mathieu and ana on 09/04/15.
 */
object DynamicTimeWrapping {

  def dtwSimpleTopK(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                    parameters: List[Double]): RDD[String] = {
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else {
      x._1 < y._1
    }

    data.sparkContext.parallelize(
      retrieveTopK(parameters.head.toInt, dynamicTimeWrappingMetric, data, testedWord, order, parameters.tail))
  }

  /**
   * Main method for comparison using Dynamic Time Wrapping
   * @param data collection of (word, temporal profile) to compare testedWord with
   * @param testedWord word that we want to find its similar words
   * @param parameters first argument is the threshold value, its hard to guess what will work for non-scaled words
   * @return words similar to testedWord
   */
  def dtwComparison(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                    parameters: List[Double]): RDD[String] = {
    val threshold = parameters.head
    data.flatMap(x => if (dynamicTimeWrappingMetric(testedWord._2, x._2) <= threshold && !x._1.equals(testedWord._1)) {
      List(x._1)
    } else {
      List()
    })
  }

  /**
   * First scale the words with the maximal number of occurrences of that word
   * @param data collection of (word, temporal profile) to compare testedWord with
   * @param testedWord word that we want to find its similar words
   * @param parameters first argument is the threshold value, should experiment to find optimal value (after some
   *                   tests between 1.6 and 2.5 seems to give kind of good results)
   * @return words similar to testedWord
   */
  def dtwComparisonScaleMax(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                            parameters: List[Double]): RDD[String] = {
    dtwComparison(data.map(proportionalScalarMaxWord), proportionalScalarMaxWord(testedWord), parameters)
  }

  /**
   * TopK version of dtwComparisonScaleMax
   */
  def dtwComparisonScaleMaxTopK(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                parameters: List[Double]): RDD[String] = {
    dtwSimpleTopK(data.map(proportionalScalarMaxWord), proportionalScalarMaxWord(testedWord), parameters)
  }

  /**
   * First scale the words with the average number of occurrences of that word
   * @param data collection of (word, temporal profile) to compare testedWord with
   * @param testedWord word that we want to find its similar words
   * @param parameters first argument is the threshold value, should experiment to find optimal value
   * @return words similar to testedWord
   */
  def dtwComparisonScaleAvg(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                            parameters: List[Double]): RDD[String] = {
    dtwComparison(data.map(x => (x._1, proportionalScalarAverageSubstraction(x._2))), (testedWord._1, proportionalScalarAverageSubstraction(testedWord._2)), parameters)
  }

  /**
   * TopK version of dtwComparisonScaleAvg
   */
  def dtwComparisonScaleAvgTopK(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                parameters: List[Double]): RDD[String] = {
    dtwSimpleTopK(data.map(x => (x._1, proportionalScalarAverageSubstraction(x._2))), (testedWord._1, proportionalScalarAverageSubstraction(testedWord._2)), parameters)
  }

  /**
   * Compares two temporal profiles using Dynamic Time Wrapping.
   * @param word1 first temporal profile
   * @param word2 second temporal profile
   * @param parameters L(0) contains the optional parameter to specify how far form the diagonal the matching can
   *                   deviate.
   * @return distance between the two temporal profiles
   */
  def dynamicTimeWrappingMetric(word1: Array[Double], word2: Array[Double],
                                parameters: List[Double] = List(20.0)): Double = {
    val dtw = Array.ofDim[Double](word1.length, word2.length)
    val margin = parameters.head.toInt
    dtw(0)(0) = distance(word1(0), word2(0))

    for (j <- 1 to Math.min(margin, word2.length - 1)) {
      dtw(0)(j) = dtw(0)(j - 1) + distance(word1(0), word2(j))
    }

    for (i <- 1 to Math.min(margin, word1.length - 1)) {
      dtw(i)(0) = dtw(i - 1)(0) + distance(word1(i), word2(0))
    }

    for (i <- 1 until word1.length) {
      for (j <- Math.max(1, i - margin) to Math.min(word2.length - 1, i + margin)) {
        if (j <= i - margin) {
          dtw(i)(j) = distance(word1(i), word2(j)) + Math.min(dtw(i - 1)(j), dtw(i - 1)(j - 1))
        } else if (j >= i + margin) {
          dtw(i)(j) = distance(word1(i), word2(j)) + Math.min(dtw(i - 1)(j - 1), dtw(i)(j - 1))
        } else {
          dtw(i)(j) = distance(word1(i), word2(j)) + Math.min(Math.min(dtw(i - 1)(j), dtw(i - 1)(j - 1)), dtw(i)(j - 1))
        }
      }
    }
    dtw(word1.length - 1)(word2.length - 1)
  }
}