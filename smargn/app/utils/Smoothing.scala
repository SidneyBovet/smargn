package utils

import org.apache.spark.rdd.RDD


/**
 * Created by johngaspoz and Fabien on 15.04.2015.
 *
 */
object Smoothing {


  def averageByWindow(arr: Array[Double], window: Double): Array[Double] = {
    val chunks = arr.grouped(window.toInt).toArray
    chunks.map(x => x.sum / x.size)
  }

  /**
   * Given a parameter x, smooth the curve by doing the average of x points together.
   * @param data collection of (word, frequency)
   * @param smoothingValue the number of points to average together
   * @return the smoothed data
   */
  def smooth(data: RDD[(String, Array[Double])], smoothingValue: Double): RDD[(String, Array[Double])] = {
    val smoothedData = data.map(x => (x._1, averageByWindow(x._2, smoothingValue)))
    smoothedData
  }

}