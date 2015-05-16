package techniques

import org.apache.spark.rdd.RDD
import utils.Scaling._
import utils.TopK._
import utils.SubTechniques._
import utils.ComputationUtilities._

/**
 * Created by Joanna on 4/7/15.
 * Naive similarity functions that compute the similar words list of a given word based on comparison techniques
 */
object NaiveComparisons {


  val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
    x._2 < y._2
  } else x._1 < y._1

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by using its dedicated metric naiveDifferenceMetricTopK
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find, L(1) contains the accepted difference to consider a word's year similar
   * @return words that are similar
   */
  def naiveDifferenceTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val acceptedDifference = parameters(1)
    val retrievedWords = retrieveTopK(k.toInt, naiveDifferenceMetricTopK, data, testedWord, order, List(acceptedDifference))
    data.sparkContext.parallelize(retrievedWords)
  }

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by using its dedicated metric naiveDifferenceSquaredMetricTopK
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find
   * @return words that are similar
   */
  def naiveDifferenceSquaredTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val retrievedWords = retrieveTopK(k.toInt, naiveDifferenceSquaredMetricTopK, data, testedWord, order)
    data.sparkContext.parallelize(retrievedWords)
  }

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by using its dedicated metric naiveDivisionMetricTopK
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find
   * @return words that are similar
   */
  def naiveDivisionTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val retrievedWords = retrieveTopK(k.toInt, naiveDivisionMetricTopK, data, testedWord, order)
    data.sparkContext.parallelize(retrievedWords)
  }

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by using its dedicated metric naiveDivisionVarianceMetricTopK
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find
   * @return words that are similar
   */
  def naiveDivisionVarianceTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val retrievedWords = retrieveTopK(k.toInt, naiveDivisionVarianceMetricTopK, data, testedWord, order)
    data.sparkContext.parallelize(retrievedWords)
  }


  /**
   * Apply the average scaling function before calling the NaiveDifference similarity function and
   * shifts it
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find, L(1) contains the accepted difference to consider a word's year similar
   * @return words that are similar
   */
  def naiveDifferenceScalingAverageWithShifting(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    shift(data, testedWord, parameters, NaiveComparisons.naiveDivisionTopKScalingAverage, parameters(1).toInt, parameters(2).toInt)
  }


  /**
   * Apply the max scaling function before calling the NaiveDivision similarity function
   * the ratio line and shifts it
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find, L(1) contains the accepted difference to consider a word's year similar
   * @return words that are similar
   */
  def naiveDivisionScalingMaxWithShifting(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    shift(data, testedWord, parameters, NaiveComparisons.naiveDivisionTopKScalingAverage, parameters(1).toInt, parameters(2).toInt)
  }


  /**
   * Given a word, find words which curve is the inverse using the NaiveInverse technique
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find
   * @return words that are similar
   */
  def naiveInverseDifference(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                             parameters: List[Double]): RDD[(String)] = {
    NaiveComparisons.naiveDifferenceTopKScalingAverage(data, (testedWord._1, testedWord._2.reverse), parameters)
  }

  /** *******************************************************************************************************
    * Metrics
    * ******************************************************************************************************* */


  /**
   * Metric that takes into account the difference between the two arrays of words
   * @param word1Freq first temporal profile
   * @param word2Freq second temporal profile
   * @param parameters L(0) contains the accepted difference between two array value that we consider similar,
   * @return sum of differences of each element divided by size of the resulting array (filtered a priori by the accepted difference if too extreme value)
   */
  def naiveDifferenceMetricTopK(word1Freq: Array[Double], word2Freq: Array[Double], parameters: List[Double] = List(15)): Double = {
    val acceptedDifference = parameters.head
    val zipped = proportionalScalarAverageSubstraction(word1Freq).zip(proportionalScalarAverageSubstraction(word2Freq))
    val zippedDif = zipped.map(x => math.abs(x._1 - x._2))
    val trueDif = zippedDif.map(_ <= acceptedDifference).filter(_ == true)
    if (trueDif.length > 0) {
      zippedDif.sum / trueDif.length
    } else {
      zippedDif.sum
    }
  }

  /**
   * Metric that takes into account the difference between the two arrays of words in a strictful manner
   * @param word1Freq first temporal profile
   * @param word2Freq second temporal profile
   * @param parameters L(0) contains the accepted difference between two array value that we consider similar,
   * @return sum of differences squared of each element divided by size of the resulting array
   */
  def naiveDifferenceSquaredMetricTopK(word1Freq: Array[Double], word2Freq: Array[Double], parameters: List[Double] = List(15)): Double = {
    val zipped = proportionalScalarAverageSubstraction(word1Freq).zip(proportionalScalarAverageSubstraction(word2Freq))
    val zippedDif = zipped.map(x => math.abs(x._1 - x._2))
    val zippedDifSquared = zippedDif.map(x => x * x)
    zippedDifSquared.sum
  }

  /**
   * Metric that takes into account the straightness of the ratio line (i.e element of the first list divided by elements of the second) between the two arrays of words
   * @param word1Freq first temporal profile
   * @param word2Freq second temporal profile
   * @return difference of min element and max element of the "ratio line" (i.e. w1/w2 element)
   */
  def naiveDivisionMetricTopK(word1Freq: (Array[Double]), word2Freq: (Array[Double]), parameters: List[Double] = List()): Double = {
    val zipped = proportionalScalarAverageSubstraction(word1Freq).zip(proportionalScalarAverageSubstraction(word2Freq))
    val min = findMinAndMax(Array(findMinAndMax(zipped.map(_._1))._1, findMinAndMax(zipped.map(_._2))._1))._1
    val zippedWithoutZero = zipped.map(x => (x._1 + min + 1, x._2 + min + 1))
    val divided = zippedWithoutZero.map(x => math.abs(x._1 / x._2))
    val minMax = findMinAndMax(divided)
    minMax._2 - minMax._1
  }

  /**
   * Metric that takes into account the straightness of the ratio line (i.e element of the first list divided by elements of the second) between the two arrays of words
   * @param word1Freq first temporal profile
   * @param word2Freq second temporal profile
   * @return difference of min element and max element of the "ratio line" (i.e. w1/w2 element)
   */
  def naiveDivisionVarianceMetricTopK(word1Freq: (Array[Double]), word2Freq: (Array[Double]), parameters: List[Double] = List()): Double = {
    val zipped = proportionalScalarAverageSubstraction(word1Freq).zip(proportionalScalarAverageSubstraction(word2Freq))
    val min = findMinAndMax(Array(findMinAndMax(zipped.map(_._1))._1, findMinAndMax(zipped.map(_._2))._1))._1
    val zippedWithoutZero = zipped.map(x => (x._1 + min + 1, x._2 + min + 1))
    val divided = zippedWithoutZero.map(x => math.abs(x._1 / x._2))
    variance(divided)
  }


}
