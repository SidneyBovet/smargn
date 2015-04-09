package techniques

import org.apache.spark.rdd.RDD

/**
 * Created by mathieu and ana on 09/04/15.
 */
object DynamicTimeWrapping {

  /**
   * Compares two temporal profiles using Dynamic Time Wrapping.
   * @param data1 first temporal profile
   * @param data2 second temporal profile
   * @param threshold optional parameter to specify how far form the diagonal the matching can deviate.
   * @return distance between the two temporal profiles
   */
  def compare(data1: RDD[(String, Array[Double])], data2: RDD[(String, Array[Double])], threshold: Int = 5): Double = {
    compare(data1.first()._2, data2.first()._2, threshold)
  }

  def compare(word1: Array[Double], word2: Array[Double], threshold: Int = 5): Double = {
    val dtw = Array.ofDim[Double](word1.length, word2.length)

    dtw(0)(0) = distance(word1(0), word2(0))
    
    for (j <- 1 to Math.min(threshold, word2.length - 1)) {
      dtw(0)(j) = dtw(0)(j - 1) + distance(word1(0), word2(j))
    }

    for (i <- 1 to Math.min(threshold, word1.length - 1)) {
      dtw(i)(0) = dtw(i - 1)(0) + distance(word1(i), word2(0))
    }

    for (i <- 1 until word1.length) {
      for (j <- Math.max(0, i - threshold) to Math.min(word2.length - 1, i + threshold)) {
        dtw(i)(j) = distance(word1(i), word2(j)) + Math.min(Math.min(dtw(i - 1)(j), dtw(i - 1)(j - 1)), dtw(i)(j - 1))
      }
    }
    dtw(word1.length - 1)(word2.length - 1)
  }

  /**
   * @param p1 first point
   * @param p2 second point
   * @return euclidean distance between the two points
   */
  def euclideanDistance(p1: (Int, Double), p2: (Int, Double)): Double = {
    Math.sqrt(Math.pow(p1._1 - p2._1, 2) + Math.pow(p1._2 - p2._2, 2))
  }

  /**
   *
   * @param p1 first point
   * @param p2 second point
   * @return absolute distance between the two points
   */
  def distance(p1: Double, p2: Double): Double = {
    Math.abs(p1 - p2)
  }
}
