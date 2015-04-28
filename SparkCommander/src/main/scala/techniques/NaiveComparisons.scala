package techniques

import controllers.Spark
import org.apache.spark.rdd.RDD
import utils.Scaling._
import utils.SubTechniques._
import utils.TopK._

/**
 * Created by Joanna on 4/7/15.
 * Naive functions that compute the similar words list of a given word based on comparison techniques
 */
object NaiveComparisons {

  import utils.ComputationUtilities._

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by computing
   * array's elements difference
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept, L(1) contains the number of non-similar values that we accept
   * @return words that are similar
   */
  def naiveDifference(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val acceptedDifference = parameters.head
    if (parameters.size > 1) {
      val acceptedFalse = parameters(1)
      data.map(x => (testedWord, x)).map(y => (y._2._1, naiveDifferenceMetric(y._1, y._2, List(acceptedDifference, acceptedFalse)))).filter(y => y._2 != Double.MaxValue && y._1 != testedWord._1).map(_._1)
    } else {
      data.map(x => (testedWord, x)).map(y => (y._2._1, naiveDifferenceMetric(y._1, y._2, List(acceptedDifference)))).filter(y => y._2 != Double.MaxValue && y._1 != testedWord._1).map(_._1)
    }
  }

  /**
   * Compare the ratio of word's frequency with the collection word's frequency to find similar words by computing
   * the ratio line
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the straightness of the curve that we accept
   * @return words that are similar
   */
  def naiveDivision(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val acceptedDifference = parameters.head
    data.map(x => (testedWord, x)).map(y => (y._2._1, naiveDivisionMetric(y._1, y._2, List(acceptedDifference)))).filter(y => y._2 != Double.MaxValue && y._1 != testedWord._1).map(_._1)
  }

  def naiveDifferenceTopK(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val acceptedDifference = parameters(1)
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1
    val retrievedWords = retrieveTopK(k.toInt, naiveDifferenceMetricTopK, data, testedWord, order, List(acceptedDifference))
    val sc = Spark.ctx
    sc.parallelize(retrievedWords)
  }

  def naiveDifferenceTopKScalingMax(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    naiveDifferenceTopK(data.map(proportionalScalarMax), proportionalScalarMax(testedWord), parameters)
  }

  def naiveDivisionTopK(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val k = parameters.head
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1
    val retrievedWords = retrieveTopK(k.toInt, naiveDivisionMetricTopK, data, testedWord, order)
    val sc = Spark.ctx
    sc.parallelize(retrievedWords)
  }

  /**
   * Apply the max scaling function before calling the NaiveDifference similarity function
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept
   * @return words that are similar
   */
  def naiveDifferenceScalingMax(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    naiveDifference(data.map(proportionalScalarMax), proportionalScalarMax(testedWord), parameters)
  }

  /**
   * Apply the average scaling function before calling the NaiveDifference similarity function
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept
   * @return words that are similar
   */
  def naiveDifferenceScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    naiveDifference(data.map(proportionalScalarAverage), proportionalScalarAverage(testedWord), parameters)
  }

  /**
   * Apply the max scaling function before calling the NaiveDivision similarity function
   * the ratio line
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the straightness of the curve that we accept
   * @return words that are similar
   */
  def naiveDivisionScalingMax(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    naiveDivision(data.map(proportionalScalarMax), proportionalScalarMax(testedWord), parameters)
  }

  /**
   * Apply the average scaling function before calling the NaiveDivision similarity function
   * the ratio line
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the straightness of the curve that we accept
   * @return words that are similar
   */
  def naiveDivisionScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    naiveDivision(data.map(proportionalScalarAverage), proportionalScalarAverage(testedWord), parameters)
  }

  /**
   * Apply the max scaling function before calling the NaiveDifference similarity function and
   * shifts it
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept, L(1) is the shifting range and L(2) is the shifting step size
   * @return words that are similar
   */
  def naiveDifferenceScalingMaxWithShifting(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    shift(data, testedWord, parameters, NaiveComparisons.naiveDifferenceScalingMax, parameters(1).toInt, parameters(2).toInt)
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
    shift(data, testedWord, parameters, NaiveComparisons.naiveDifferenceScalingAverage, parameters(1).toInt, parameters(2).toInt)
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
    shift(data, testedWord, parameters, NaiveComparisons.naiveDivisionScalingMax, parameters(1).toInt, parameters(2).toInt)
  }

  /**
   * Apply the average scaling function before calling the NaiveDivision similarity function
   * the ratio line and shifts it
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the straightness of the curve that we accept, L(1) is the shifting range and L(2) is the shifting step size
   * @return words that are similar
   */
  def naiveDivisionScalingAverageWithShifting(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    shift(data, testedWord, parameters, NaiveComparisons.naiveDivisionScalingAverage, parameters(1).toInt, parameters(2).toInt)
  }

  /** *******************************************************************************************************
    * Metrics
    * ******************************************************************************************************* */
  /**
   * Compute the metric of similarity based on a naiveDifference approach for two words.
   * @param word1
   * @param word2
   * @param parameters L(0) contains the accepted difference between two array value that we accept and L(1) the percentage of non-similar values that we accept
   * @return sum of differences of each element if words are considered similar, Double.MaxValue otherwise
   */
  def naiveDifferenceMetric(word1: (String, Array[Double]), word2: (String, Array[Double]), parameters: List[Double] = List(15, 0.05)): Double = {
    val acceptedDifference = parameters.head
    var acceptedFalse = 0.05
    if (parameters.size > 1) {
      acceptedFalse = parameters(1)
    }
    val zipped = word1._2.zip(word2._2)
    val zippedDif = zipped.map(x => math.abs(x._1 - x._2))
    if (zippedDif.map(_ <= acceptedDifference).count(_ == false) <= zippedDif.size * acceptedFalse) {
      zippedDif.sum
    } else {
      Double.MaxValue
    }
  }

  //problem metric only base on 2 points ==> very biased
  /**
   * Compute the metric of similarity based on a naiveDivision approach for two words.
   * @param word1
   * @param word2
   * @param parameters L(0) contains the accepted difference between min and max value of the created line
   * @return difference of min element and max element of the "line" (i.e. w1/w2 element) if words are considered similar, Double.MaxValue otherwise
   */
  def naiveDivisionMetric(word1: (String, Array[Double]), word2: (String, Array[Double]), parameters: List[Double] = List(0.8)): Double = {
    val acceptedDifference = parameters.head
    val zipped = word1._2.zip(word2._2)
    val divided = zipped.map(x => math.abs((if (x._1 == 0) {
      if (x._2 == 0) {
        1
      } else {
        x._2 * x._2
      }
    }
    else {
      x._1
    }) / (if (x._2 == 0) {
      1
    } else {
      x._2
    })))
    val minMax = findMinAndMax(divided)
    if (minMax._2 - minMax._1 < acceptedDifference) {
      minMax._2 - minMax._1
    } else {
      Double.MaxValue
    }
  }

  def naiveDifferenceMetricTopK(word1: (String, Array[Double]), word2: (String, Array[Double]), parameters: List[Double] = List(15)): Double = {
    val acceptedDifference = parameters.head
    val zipped = word1._2.zip(word2._2)
    val zippedDif = zipped.map(x => math.abs(x._1 - x._2))
    val trueDif = zippedDif.map(_ <= acceptedDifference).filter(_ == true)
    if (trueDif.length > 0) {
      zippedDif.sum / trueDif.length
    } else {
      zippedDif.sum
    }
  }

  def naiveDivisionMetricTopK(word1: (String, Array[Double]), word2: (String, Array[Double]), parameters: List[Double] = List()): Double = {
    val zipped = word1._2.zip(word2._2)
    val divided = zipped.map(x => math.abs((if (x._1 == 0) {
      if (x._2 == 0) {
        1
      } else {
        x._2 * x._2
      }
    }
    else {
      x._1
    }) / (if (x._2 == 0) {
      1
    } else {
      x._2
    })))
    val minMax = findMinAndMax(divided)
    minMax._2 - minMax._1
  }


}