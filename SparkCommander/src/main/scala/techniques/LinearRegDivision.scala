package techniques

import org.apache.spark.rdd.RDD
import utils.ComputationUtilities._
import utils.Scaling._
import utils.TopK._


/**
 * Created by Joanna on 10.05.15.
 * @author Joanna Salathé
 *         Similarity functions that compute the similar words list of a given word based on simple linear regression
 */
object LinearRegDivision {

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by using its dedicated metric linearRegDivisionMetricTopK
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param parameters L(0) contains the number of similar words we want to find,
   * @return words that are similar
   */
  def linearRegDivisionTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1
    val retrievedWords = retrieveTopK(k.toInt, linearRegDivisionMetricTopK, data, testedWord, order)
    data.sparkContext.parallelize(retrievedWords)
  }

  /**
   * Metric that takes into account the straightness of the ratio line (i.e element of the first list divided by elements of the second) between the two arrays of words
   * @param word1Freq first temporal profile
   * @param word2Freq second temporal profile
   * @return slope of the ration line computed with the help of simple linear regression technique
   */
  //http://en.wikipedia.org/wiki/Simple_linear_regression
  def linearRegDivisionMetricTopK(word1Freq: (Array[Double]), word2Freq: (Array[Double]), parameters: List[Double] = List()): Double = {
    val zipped = proportionalScalarAverageSubstraction(word1Freq).zip(proportionalScalarAverageSubstraction(word2Freq))
    val min = findMinAndMax(Array(findMinAndMax(zipped.map(_._1))._1, findMinAndMax(zipped.map(_._2))._1))._1
    val zippedWithoutZero = zipped.map(x => (x._1 + min + 1, x._2 + min + 1))
    val divided = zippedWithoutZero.map(x => math.abs(x._1 / x._2))
    val cs = divided.zipWithIndex.map(x => (x._2, x._1))
    val xs = cs.map(_._1)
    val ys = cs.map(_._2)
    val xys = cs.map(c => c._1 * c._2)
    val xmean = xs.sum / xs.length
    val ymean = ys.sum / ys.length
    val xymean = xys.sum / xys.length
    val xssq = xs.map(x => x * x)
    val xssqmean = xssq.sum / xssq.length
    val num = xmean * ymean - xys.sum / xys.length
    val denom = xmean * xmean - xssqmean
    math.abs(num / denom)

  }
}
