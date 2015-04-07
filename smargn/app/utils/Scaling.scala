package utils

/**
 * Created by Joanna on 4/7/15.
 * Functions used to scale the curves before applying any similarity functions
 */
object Scaling {

  import utils.ComputationUtilities._

  /**
   * Scale the word, frequency tuple with the help of its maximum frequency
   * @param wordFreq tuple of word and its frequency
   * @return tuple of word and its max-scaled frequency
   */
  def proportionalScalarMax(wordFreq: (String, Array[Double])): (String, Array[Double]) = {
    val max = findMinAndMax(wordFreq._2)._2
    (wordFreq._1, wordFreq._2.map(x => x / max))
  }

  /**
   * Scale the word, frequency tuple with the help of its average frequency
   * @param formattedWord tuple of word and its frequency
   * @return tuple of word and its average-scaled frequency
   */
  def proportionalScalarAverage(formattedWord: (String, Array[Double])): (String, Array[Double]) = {
    val sum = formattedWord._2.sum
    (formattedWord._1, formattedWord._2.map(x => x / sum))
  }
}
