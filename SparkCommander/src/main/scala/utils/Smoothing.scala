package utils

import org.apache.spark.rdd.RDD


/**
 * Created by johngaspoz and Fabien on 15.04.2015.
 *
 */
object Smoothing {


  def averageByWindow(arr: Array[Double], window: Double): Array[Double] = {
    var smoothed: Array[Double] = Array(arr.head)
    var from: Int = 0
    var to: Int = window.toInt
    var i = 0

    for (i <- 0 to (arr.length - window.toInt)) {

      val smoothPoint: Double = arr.slice(from, to).sum / arr.slice(from, to).length
      smoothed = smoothed :+ smoothPoint
      from = from + 1
      to = to + 1


    }
    smoothed = smoothed :+ arr.last
    smoothed
  }

  /**
   * Given a parameter x, smooth the curve by doing the average of x points together.
   * @param data collection of (word, frequency)
   * @param smoothingValue the number of points to average together if window = x : x points will be averaged together
   * @return the smoothed data
   */
  def smooth(data: RDD[(String, Array[Double])], smoothingValue: Double): RDD[(String, Array[Double])] = {
    val smoothedData = data.map(x => (x._1, averageByWindow(x._2, smoothingValue)))
    smoothedData
  }

  /**
   * Given a parameter x, smooth the curve by doing the average of x points together.
   * @param data collection of (word, frequency)
   * @param smoothingValue the number of points to average together if window = x : x points will be averaged together
   * @return the smoothed data
   */
  def smooth(data: (String, Array[Double]), smoothingValue: Double): (String, Array[Double]) = {
    val smoothedData = (data._1, averageByWindow(data._2, smoothingValue))
    smoothedData
  }

}