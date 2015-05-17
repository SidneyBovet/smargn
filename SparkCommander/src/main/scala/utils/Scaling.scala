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
   * Scale the frequency of the word with the help of its average frequency by division
   * @param frequency tuple of word and its frequency
   * @return tuple of word and its average-scaled frequency
   */
  def proportionalScalarAverageDivision(frequency: (Array[Double])): (Array[Double]) = {
    val avg = frequency.sum / frequency.length
    frequency.map(x => x / avg)
  }

  /**
   * Scale the frequency of the word with the help of its average frequency by substraction
   * @param frequency tuple of word and its frequency
   * @return tuple of word and its average-scaled frequency
   */
  def proportionalScalarAverageSubstraction(frequency: (Array[Double])): (Array[Double]) = {
    val avg = frequency.sum / frequency.length
    frequency.map(x => x - avg)
  }


}
