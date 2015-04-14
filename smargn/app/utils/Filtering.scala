package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created with love by sidney.
 *
 * Useful methods to detect interesting temporal profiles.
 * Look at the 'interestingWords' method for a complete example.
 */
object Filtering {

  /**
   *
   * @param a The array to compute the mean of
   * @return The mean of the array a, or 0 if a is empty
   */
  def mean(a: Array[Double]): Double = {
    if (a.length == 0) {
      0
    } else {
      a.sum / a.length
    }
  }

  /**
   * Return whether the array has (any) interesting points
   * @param array the temporal profile of a word
   * @param threshold the threshold value to discriminate normal points
   * @return true if any point in array goes too far from the mean
   */
  def isNotFlat(array: Array[Double], threshold: Double): Boolean = {
    val meanVal = mean(array)
    array.foldLeft(false)((b: Boolean, el: Double) => {
      b || ((el - meanVal) * (el - meanVal) > threshold)
    })
  }

  /**
   * Return the number of years in which the array has interesting points
   * @param array the temporal profile of a word
   * @param threshold the threshold value to discriminate normal points
   * @return true if any point in array goes too far from the mean
   */
  def countNotFlat(array: Array[Double], threshold: Double): Int = array.foldLeft(0)((acc: Int, el: Double) => {
    val meanVal = mean(array)
    if ((el - meanVal) * (el - meanVal) > threshold) {
      acc + 1
    } else {
      acc
    }
  })

  /**
   * Example function to show how to discriminate flat profiles
   * @param inputFile File to read the profiles from.
   * @param sc SparkContext attached to the application.
   * @param threshold A positive threshold value. The lower the more words are output.
   * @return A list of words having unusually small or big values during one or more year(s).
   */
  def interestingWords(inputFile: String, sc: SparkContext, threshold: Double): RDD[String] = {
    val kvPairs = Formatting.dataFormatter(sc.textFile(inputFile)).cache()
    val kvPairsNorm = kvPairs.map(Scaling.proportionalScalarAverage)
    kvPairsNorm.flatMap(t => {
      if (isNotFlat(t._2, 1)) {
        List(t._1)
      } else {
        List()
      }
    })
  }

}
