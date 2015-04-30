package utils

/**
 * Created by Joanna on 4/7/15.
 * Utilities useful for computing statistics on the words
 */
object ComputationUtilities {

  /**
   * Find the min and max of an array
   * @param array array we want to find its min and max
   * @return tuple of min and max of the array
   */
  //taken from http://stackoverflow.com/questions/20285209/find-min-and-max-elements-of-array
  def findMinAndMax(array: Array[Double]): (Double, Double) = {
    // a non-empty array
    if (array.length != 0) {
      val initial = (array.head, array.head) // a tuple representing min-max
      array.foldLeft(initial) { (acc, x) =>
        if (x < acc._1) {
          (x, acc._2)
        } else if (x > acc._2) {
          (acc._1, x)
        } else {
          acc
        }
      }
    } else {
      (-1.0, -1.0)
    }
  }

  /**
   *
   * @param array
   * @return
   */
  def average(array: Array[Double]): Double = {
    if (array.length == 0) {
      0
    } else {
      array.sum / array.length
    }
  }

  /**
   *
   * @param array
   * @return variance of the sample values
   */
  def variance(array: Array[Double]): Double = {
    if (array.length <= 1) {
      0
    } else {
      val mean = average(array)
      array.map { x => (x - mean) * (x - mean) }.sum / (array.length - 1)
    }
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
   * @param p1 first point
   * @param p2 second point
   * @return absolute distance between the two points
   */
  def distance(p1: Double, p2: Double): Double = {
    Math.abs(p1 - p2)
  }
}
