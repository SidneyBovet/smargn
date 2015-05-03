package utils

/**
 * Created by Joanna on 4/7/15.
 * Functions used to scale the curves before applying any similarity functions
 */
object Scaling {

  import utils.ComputationUtilities._

  /**
   * Scale the word, frequency tuple with the help of its maximum frequency
   * @param word tuple of word and its frequency
   * @return tuple of word and its max-scaled frequency
   */
  def proportionalScalarMaxWord(word: (String, Array[Double])): (String, Array[Double]) = {
    val max = findMinAndMax(word._2)._2
    (word._1, word._2.map(x => x / max))
  }

  /**
   * Scale the word, frequency tuple with the help of its average frequency
   * @param word tuple of word and its frequency
   * @return tuple of word and its average-scaled frequency
   */
  def proportionalScalarAverageWord(word: (String, Array[Double])): (String, Array[Double]) = {
    val sum = word._2.sum
    (word._1, word._2.map(x => x / sum))
  }


  /**
   * Scale the frequency of the word with the help of its average frequency
   * @param frequency tuple of word and its frequency
   * @return tuple of word and its average-scaled frequency
   */
  def proportionalScalarAverage(frequency: (Array[Double])): (Array[Double]) = {
    val sum = frequency.sum
    frequency.map(x => x / sum)
  }
  

}
