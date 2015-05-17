package techniques

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import utils.TopK._

/**
 * This object contains the peak definitions and the metrics that use them. (with all the helpers)
 * @author Zhivka Gucevska mainly and help from Joanna Salathé at the beginning and Ana Manasovska at the end of the project
 */
object PeakComparison {

  /**
   * This function finds the top K elements similar to the given word according to the peak metric
   *
   * @param data All the data on the cluster
   * @param testedWord The word we are interested in
   * @param parameters The parameters:
   *                   List(0) - the "smoothing" window
   *                   List(1) - the ration between minimum and maximum that defines a peak
   * @return List of the top K words similar to testedWord (not sure if it's sorted)
   *
   */
  def peaksTopK(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                parameters: List[Double]): RDD[String] = {
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else {
      x._1 < y._1
    }

    data.sparkContext.parallelize(
      retrieveTopK(parameters.head.toInt, peakMeanDerivativeMetric, data, testedWord, order, parameters.tail))
  }

  /**
   * Compute the similar words using the derivative peak metric
   *
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) - contains the threshold of considering two words similar
   *                   L(1) - contains the windows size,
   *                   L(2) - the ration between minimum and maximum that defines a peak
   *
   * @return The list of words that satisfy the given threshold
   */
  def peakComparisonWithMeanDerivative(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                       parameters: List[Double]): RDD[(String)] = {
    //TODO change the name
    val proportionSimilarities = parameters.head
    val shift = parameters(1).toInt
    val deltaY = if (parameters.size < 3) 3 else parameters(2)

    data.flatMap { x =>
      if (peakMeanDerivativeMetric(testedWord._2, x._2, List(shift, deltaY)) < proportionSimilarities) {
        List(x._1)
      } else {
        List()
      }
    }
  }

  /**
   *
   * @param words List of words which peaks are returned
   * @param parameters list of parameters for the peak finder
   * @return for each word is returned the a list of peaks represented in a tuple with meaning (PeakIndex,
   *         LeftMinValue, RightMinValue)
   */
  def getPeaks(words: RDD[(String, Array[Double])],
               parameters: List[Double]): RDD[(String, List[(Int, Double, Double)])] = {
    // val proportionSimilarities = parameters.head
    val windowSize = if (parameters.nonEmpty) parameters.head.toInt else 3
    val deltaY = if (parameters.size < 2) 3 else parameters(1)
    words.map(x => (x._1, testPeak(x._2, deltaY).map(y => (y._1 + 1840, y._2 + 1840, y._3 + 1840)).sortBy(_._1)))

  }

  /** *******************************************************************************************************
    * Metrics
    * ******************************************************************************************************* */
  /**
   *
   * @param word1
   * @param word2
   * @param params
   * @return
   */
  def peakMeanDerivativeMetric(word1: Array[Double], word2: Array[Double],
                               params: List[Double] = List(3, 3)): Double = {
    val shift = params.head.toInt
    val deltaY = params(1)
    val distinctPeaks1 = testPeak(word1, deltaY)
    val distinctPeaks2 = testPeak(word2, deltaY)

    if (distinctPeaks1.size == 0 || distinctPeaks2.size == 0) {
      return 1.0
    }

    val numberOfSimilarPeaks = (for {p1 <- distinctPeaks1
                                     p2 <- distinctPeaks2
                                     if Math.abs(p1._1 - p2._1) <= Math.max(0, shift)} yield {
      p1
    }).size

    1 - Math.min(numberOfSimilarPeaks * 1.0 / distinctPeaks1.size, numberOfSimilarPeaks * 1.0 / distinctPeaks2.size)
  }

  def testPeak(word: Array[Double], deltaY: Double): List[(Int, Double, Double)] = {
    val peaksLeft = peakMeanDerivative(word, deltaY)
    val peaksRight = peakMeanDerivative(word.reverse, deltaY)
    val size = word.length
    //join the windows of the peaks so that we can have as precise values as possible of the window where the peak is
    val peaks = for {p1 <- peaksLeft
                     p2 <- peaksRight
                     if p1._1 == size - p2._1 - 1} yield {
      (p1._1, p1._2, size - 1 - p2._2)
    }

    peaks
  }

  /**
   * Filters out the duplicate years, by keeping for each peak the maximum delta/slope detected
   * @param l
   * @return
   */
  def filterDuplicateYears(l: List[(Int, Double, Double)]): List[(Int, Double, Double)] = {
    l.groupBy(x => x._1).toList.map { t =>
      val left = t._2.map(x => x._2).max
      val right = t._2.map(x => Math.abs(x._3)).max

      (t._1, left, right)
    }
  }

  /** *******************************************************************************************************
    * Peak definitions
    * ******************************************************************************************************* */
  /**
   * For a given word detects the peaks based on finding the largest ascending window (with some smoothing based on
   * mean), takes the maximum in that window, compares it against the minimum in the ascending window, and then finds
   * the largest descending window starting from the maximum, and once again compares the maximum to the minimum in the
   * window. If the respective differences satisfy the threshold, the maximum is detected as a peak
   *
   *
   * @param word word the size of the smoothing window
   * @param deltaY from 2 to 5
   * @return list of peaks: (Year, left slope, right slope). This list may contain duplicate years and should be
   *         filtered
   *         before any usage
   */
  def peakMeanDerivative(word: Array[Double], deltaY: Double = 3): List[(Int, Double, Double)] = {
    val frequencies = word
    var result = List[(Int, Double, Double)]()
    //val average = word.sum / word.length
    var i = 0
    while (i < frequencies.length) {
      val ascendingWindow = frequencies.slice(i, findAscendingAverageWindow(frequencies, i) + 1)
      if (ascendingWindow.nonEmpty) {
        val indexOfMaxAscendingInWindow = ascendingWindow.indexOf(ascendingWindow.max)
        val indexOfMaxAscending = indexOfMaxAscendingInWindow + i

        val minAscending = ascendingWindow.slice(0, indexOfMaxAscendingInWindow + 1).min

        val indexOfMinAscending = ascendingWindow.slice(0, indexOfMaxAscendingInWindow + 1).indexOf(minAscending) + i

        //check if the detected window is of size 1 or the detaX is too large
        val dyLeft = frequencies(indexOfMaxAscending) / frequencies(indexOfMinAscending);
        val dxLeft = indexOfMaxAscending - indexOfMinAscending
        if (minAscending != frequencies(indexOfMaxAscending) && dyLeft * dyLeft > dxLeft
          && frequencies(indexOfMaxAscending) > deltaY * frequencies(indexOfMinAscending)) {
          val descendingWindow = frequencies
            .slice(indexOfMaxAscending, findDescendingAverageWindow(frequencies, indexOfMaxAscending) + 1)
          //check is the detected window is of size 1
          if (descendingWindow.size > 1) {
            val indexOfMinDescending = descendingWindow.indexOf(descendingWindow.min) + indexOfMaxAscending
            val dyRight = frequencies(indexOfMaxAscending) / frequencies(indexOfMinDescending);
            val dxRight = indexOfMinDescending - indexOfMaxAscending
            if (dyRight * dyRight > dxRight &&
              frequencies(indexOfMaxAscending) > deltaY * frequencies(indexOfMinAscending)) {
              result = (indexOfMaxAscending, indexOfMinAscending * 1.0, indexOfMinDescending * 1.0) :: result
            }
            i = indexOfMinDescending
          } else {
            i += 1
          }
        } else {
          i += 1
        }
      } else {
        i += 1
      }
    }
    result.reverse
  }

  /**
   * Finds the maximum index such that the average of the points in a given window size is increasing. Returns that
   * index
   *
   * @param frequencies the array we are considering
   * @return the position of the last ascending average value
   */
  private def findAscendingAverageWindow(frequencies: Array[Double], start: Int): Int = {
    var lastAverage = 0.0
    for (i <- start + 1 to frequencies.length) {
      val average = frequencies.slice(start, i).sum * 1.0 / (i - start)
      if (lastAverage < average) {
        lastAverage = average
      } else {
        return i - 1
      }
    }
    frequencies.length - 1
  }

  /**
   * Finds the maximum index such that the average of the points in a given window size is decreasing. Returns that
   * index
   *
   * @param frequencies the array we are considering
   * @return the position of the last descending average value
   */
  private def findDescendingAverageWindow(frequencies: Array[Double], start: Int): Int = {
    var lastAverage = frequencies(start)
    for (i <- start + 1 to frequencies.length) {
      val average = frequencies.slice(start, i).sum * 1.0 / (i - start)
      if (lastAverage >= average) {
        lastAverage = average
      } else {
        return i - 1
      }
    }
    frequencies.length - 1
  }

  def findPeakCurves(data: RDD[(String, Array[Double])], strength: Double): RDD[String] = {
    data.flatMap { w =>
      if (isPeakCurve(w, strength) < 1) {
        List(w._1)
      } else {
        List()
      }
    }
  }

  def isPeakCurve(word: (String, Array[Double]), strength: Double): Double = {
    val peaks = peakMeanDerivative(word._2, 2)
    val mean = word._2.sum / word._2.size

    if (peaks.size == 0) {
      return 1.0
    }

    val strongPeaks = peaks.foldLeft(1)((a, b) => if (word._2(b._1) >= strength * mean) a + 1 else a)

    1 - strongPeaks * 1.0 / peaks.size
  }
}
