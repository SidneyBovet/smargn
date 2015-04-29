package techniques

import org.apache.spark.rdd.RDD

/**
 * Created by John and Fabien on 13.4.15.
 * Naive functions that compute the similar words list of a given word based on comparison techniques
 */
object NaiveInverseComparisons {

  import utils.SubTechniques._

  /**
   * Given a word, find words which curve is the inverse using the NaiveInverse technique
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the accepted difference between two array value that we accept
   * @return words that have inversed curve.
   */
  def naiveInverseDifference(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                             parameters: List[Double]): RDD[(String)] = {
    NaiveComparisons.naiveDifference(data, (testedWord._1, testedWord._2.reverse), parameters)
  }

}