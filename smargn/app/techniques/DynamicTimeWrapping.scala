package techniques

import org.apache.spark.rdd.RDD
import utils.Scaling._

/**
 * Created by mathieu and ana on 09/04/15.
 */
object DynamicTimeWrapping {

  /**
   * Main method for comparison using Dynamic Time Wrapping
   * @param data collection of (word, temporal profile) to compare testedWord with
   * @param testedWord word that we want to find its similar words
   * @param parameters first argument is the threshold value, its hard to guess what will work for non-scaled words
   * @return words similar to testedWord
   */
  def dtwComparison(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                    parameters: List[Double]): RDD[(String)] = {
    val threshold = parameters.head
    data.flatMap(
      x => if (dynamicTimeWrappingMetric(testedWord._2, x._2) <= threshold && !x._1.equals(testedWord._1)) {
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
                            parameters: List[Double]): RDD[(String)] = {
    dtwComparison(data.map(proportionalScalarMax), proportionalScalarMax(testedWord), parameters)
  }

  /**
   * First scale the words with the average number of occurrences of that word
   * @param data collection of (word, temporal profile) to compare testedWord with
   * @param testedWord word that we want to find its similar words
   * @param parameters first argument is the threshold value, should experiment to find optimal value
   * @return words similar to testedWord
   */
  def dtwComparisonScaleAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                parameters: List[Double]): RDD[(String)] = {
    dtwComparison(data.map(proportionalScalarAverage), proportionalScalarAverage(testedWord), parameters)
  }

  /**
   * Compares two temporal profiles using Dynamic Time Wrapping.
   * @param word1 first temporal profile
   * @param word2 second temporal profile
   * @param margin optional parameter to specify how far form the diagonal the matching can deviate.
   * @return distance between the two temporal profiles
   */
  def dynamicTimeWrappingMetric(word1: Array[Double], word2: Array[Double], margin: Int = 20): Double = {
    val dtw = Array.ofDim[Double](word1.length, word2.length)

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

  /**
   * @param p1 first point
   * @param p2 second point
   * @return euclidean distance between the two points
   */
  def euclideanDistance(p1: (Int, Double), p2: (Int, Double)): Double = {
    Math.sqrt(Math.pow(p1._1 - p2._1, 2) + Math.pow(p1._2 - p2._2, 2))
  }

  /**
   * @param p1 first point
   * @param p2 second point
   * @return absolute distance between the two points
   */
  def distance(p1: Double, p2: Double): Double = {
    Math.abs(p1 - p2)
  }
}
