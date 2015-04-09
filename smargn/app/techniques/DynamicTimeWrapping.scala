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
    val word1 = data1.first()
    val word2 = data2.first()
    val dtw = Array.ofDim[Double](word1._2.length, word2._2.length)
    dtw(0)(0) = euclideanDistance((0, word1._2(0)), (0, word2._2(0)))

    for (i <- 1 to word1._2.length) {
      for (j <- Math.max(0, i - threshold) to Math.min(word2._2.length, i + threshold)) {

      }
    }
    0.0
  }

  /**
   * @param p1 first point
   * @param p2 second point
   * @return euclidean distance between the two points
   */
  def euclideanDistance(p1: (Int, Double), p2: (Int, Double)): Double = {
    Math.sqrt(Math.pow(p1._1 - p2._1, 2) + Math.pow(p1._2 - p2._2, 2))
  }
}
