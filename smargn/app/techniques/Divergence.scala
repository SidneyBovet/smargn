package techniques

import org.apache.spark.rdd.RDD

/**
 * Created by johngaspoz on 19.04.15
 * Naive functions that compute the similar shifted words list of a given word based on comparison techniques
 */
object Divergence {

  import utils.SubTechniques._

  /**
   * NaiveDifference shifted 3 times
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept
   *
   * @return words that are similar but shifted between 0 and 3 times (right or left)
   */
  def naiveDifferenceDivergence(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                parameters: List[Double]): RDD[(String)] = {
    
    divergence(data, testedWord, parameters, NaiveComparisons.naiveDifferenceScalingMax)
  }

}