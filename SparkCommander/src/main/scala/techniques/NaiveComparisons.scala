package techniques

import org.apache.spark.rdd.RDD
import utils.Scaling._
import utils.TopK._
import utils.SubTechniques._

/**
 * Created by Joanna on 4/7/15.
 * Naive functions that compute the similar words list of a given word based on comparison techniques
 */
object NaiveComparisons {

  import utils.ComputationUtilities._

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by computing
   * array's elements difference. It first scales everybody by the average and the output a ranked list of similar words
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept, L(1) contains the number of non-similar values that we accept
   * @return words that are similar
   */
  def naiveDifferenceTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val acceptedDifference = parameters(1)
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1
    val retrievedWords = retrieveTopK(k.toInt, naiveDifferenceMetricTopK, data, testedWord, order, List(acceptedDifference))
    data.sparkContext.parallelize(retrievedWords)
  }

  /**
   * Compare the ratio of word's frequency with the collection word's frequency to find similar words by computing
   * the ratio line. It first scales everybody by the average and the output a ranked list of similar words
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the straightness of the curve that we accept
   * @return words that are similar
   */
  def naiveDivisionTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1
    val retrievedWords = retrieveTopK(k.toInt, naiveDivisionMetricTopK, data, testedWord, order)
    data.sparkContext.parallelize(retrievedWords)
  }


  /**
   * Apply the average scaling function before calling the NaiveDifference similarity function and
   * shifts it
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept, L(1) is the shifting range and L(2) is the shifting step size
   * @return words that are similar
   */
  def naiveDifferenceScalingAverageWithShifting(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    shift(data, testedWord, parameters, NaiveComparisons.naiveDivisionTopKScalingAverage, parameters(1).toInt, parameters(2).toInt)
  }


  /**
   * Apply the max scaling function before calling the NaiveDivision similarity function
   * the ratio line and shifts it
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the straightness of the curve that we accept, L(1) is the shifting range and L(2) is the shifting step size
   * @return words that are similar
   */
  def naiveDivisionScalingMaxWithShifting(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    shift(data, testedWord, parameters, NaiveComparisons.naiveDivisionTopKScalingAverage, parameters(1).toInt, parameters(2).toInt)
  }


  /**
   * Given a word, find words which curve is the inverse using the NaiveInverse technique
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept
   * @return words that have inversed curve.
   */
  def naiveInverseDifference(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                             parameters: List[Double]): RDD[(String)] = {
    NaiveComparisons.naiveDifferenceTopKScalingAverage(data, (testedWord._1, testedWord._2.reverse), parameters)
  }

  /** *******************************************************************************************************
    * Metrics
    * ******************************************************************************************************* */


  /**
   *
   * @param word1 first temporal profile
   * @param word2 second temporal profile
   * @param parameters L(0) contains the accepted difference between two array value that we accept,
   * @return sum of differences of each element divided by size of the resulting array (filtered a priori by the accepted difference if too extreme value)
   */
  def naiveDifferenceMetricTopK(word1: (String, Array[Double]), word2: (String, Array[Double]), parameters: List[Double] = List(15)): Double = {
    val acceptedDifference = parameters.head
    val zipped = proportionalScalarAverage(word1._2).zip(proportionalScalarAverage(word2._2))
    val zippedDif = zipped.map(x => math.abs(x._1 - x._2))
    val trueDif = zippedDif.map(_ <= acceptedDifference).filter(_ == true)
    if (trueDif.length > 0) {
      zippedDif.sum / trueDif.length
    } else {
      zippedDif.sum
    }
  }


  /**
   *
   * @param word1 first temporal profile
   * @param word2 second temporal profile
   * @return difference of min element and max element of the "line" (i.e. w1/w2 element)
   */
  def naiveDivisionMetricTopK(word1: (String, Array[Double]), word2: (String, Array[Double]), parameters: List[Double] = List()): Double = {
    val zipped = proportionalScalarAverage(word1._2).zip(proportionalScalarAverage(word2._2))
    val zippedWithoutZero = zipped.map(x => (x._1 + 1, x._2 + 1))
    val divided = zippedWithoutZero.map(x => math.abs(x._1 / x._2))
    val minMax = findMinAndMax(divided)
    minMax._2 - minMax._1
  }


}
