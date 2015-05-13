package techniques

import org.apache.spark.rdd.RDD
import utils.ComputationUtilities._
import utils.Scaling._
import utils.TopK._
import org.apache.log4j.Logger


/**
 * Created by Joanna on 10.05.15.
 */
object LinearRegDivision {
  val log = Logger.getLogger(getClass.getName)

  def linearRegDivisionTopKScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    log.info("FIND ME parametersLinearRegDivison " + parameters.toString())
    val k = parameters.head
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1
    val retrievedWords = retrieveTopK(k.toInt, linearRegDivisionMetricTopK, data, testedWord, order)
    data.sparkContext.parallelize(retrievedWords)
  }

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
