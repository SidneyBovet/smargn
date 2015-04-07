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

}
