package techniques

import org.apache.spark.rdd.RDD
import utils.Scaling._

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
    if(parameters.size>1) {
      val acceptedFalse = parameters(1)
      data.map(x => (testedWord, x)).map(y => (y._2._1, naiveDifferenceMetric(y._1, y._2, acceptedDifference, acceptedFalse))).filter(y => y._2 != Double.MaxValue && y._1 != testedWord._1).map(_._1)
    }else{
      data.map(x => (testedWord, x)).map(y => (y._2._1, naiveDifferenceMetric(y._1, y._2, acceptedDifference))).filter(y => y._2 != Double.MaxValue && y._1 != testedWord._1).map(_._1)
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
    data.map(x=> (testedWord, x)).map(y=>(y._2._1, naiveDivisionMetric(y._1, y._2, acceptedDifference))).filter(_._2 != Double.MaxValue).map(_._1)
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




  /** *******************************************************************************************************
    * Metrics
    * ******************************************************************************************************* */


  /**
   * Compute the metric of similarity based on a naiveDifference approach for two words.
   * @param word1
   * @param word2
   * @param acceptedDifference accepted difference between two array value that we accept
   * @param acceptedFalse he number of non-similar values that we accept
   * @return sum of differences of each element if words are considered similar, Double.MaxValue otherwise
   */
  def naiveDifferenceMetric(word1: (String, Array[Double]), word2: (String, Array[Double]), acceptedDifference: Double = 10, acceptedFalse: Double = 1): Double = {

    val zipped = word1._2.zip(word2._2)
    val zippedDif = zipped.map(x=>math.abs(x._1-x._2))
    if(zippedDif.map(_<= acceptedDifference).count(_==false)<=acceptedFalse){
      zippedDif.sum
    }
    else{
      Double.MaxValue
    }
  }

  //problem metric only base on 2 points ==> very biased
  /**
   * Compute the metric of similarity based on a naiveDivision approach for two words.
   * @param word1
   * @param word2
   * @param acceptedDifference accepted difference between two array value that we accept
   * @return difference of min element and max element of the "line" (i.e. w1/w2 element) if words are considered similar, Double.MaxValue otherwise
   */
  def naiveDivisionMetric(word1: (String, Array[Double]), word2: (String, Array[Double]), acceptedDifference: Double = 0.2): Double = {

    val zipped = word1._2.zip(word2._2)
    val divided = zipped.map(x=>math.abs((if(x._1==0) x._2 else x._1) / (if(x._2==0) 1 else x._2)))
    val minMax = findMinAndMax(divided)
    if(minMax._2 - minMax._1 < acceptedDifference){
      minMax._2 - minMax._1
    }
    else{
      Double.MaxValue
    }
  }

}