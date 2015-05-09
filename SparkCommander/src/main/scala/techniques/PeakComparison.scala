package techniques

import org.apache.spark.rdd.RDD
import utils.ComputationUtilities._
import utils.TopK._

object PeakComparison {


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
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the threshold of considering two words similar L(1) contains the windows size, L
   *                   (2) contains the axisXDeviation, L(3) contains the deltaMinMax
   * @return
   */
  def peakComparisonWithMaxMin(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                               parameters: List[Double]): RDD[(String)] = {
    val proportionSimilarities = parameters.head
    val windowSize = parameters(1).toInt

    data.flatMap { x =>
      if (peakMaxMinMetric(testedWord, x, windowSize) > proportionSimilarities) {
        List(x._1)
      } else {
        List()
      }
    }
  }

  /**
   * Compute the similar words using the derivative peak metric
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the threshold of considering two words similar L(1) contains the windows size, L
   *                   (2) contains the axisXDeviation, L(3) contains the deltaMean
   * @return
   */
  def peakComparisonWithMean(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                             parameters: List[Double]): RDD[(String)] = {
    val proportionSimilarities = parameters.head
    val windowSize = parameters(1).toInt

    data.flatMap { x =>
      if (peakMeanMetric(testedWord, x, windowSize) > proportionSimilarities) {
        List(x._1)
      } else {
        List()
      }
    }
  }

  /**
   * Compute the similar words using the derivative peak metric
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the threshold of considering two words similar L(1) contains the windows size, L
   *                   (2) contains the axisXDeviation, L(3) contains the deltaMean
   * @return
   */
  def peakComparisonWithMeanDerivative(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                       parameters: List[Double]): RDD[(String)] = {
    val proportionSimilarities = parameters.head
    val windowSize = parameters(1).toInt
    val deltaX = parameters(2)
    val deltaY = if (parameters.size < 4) 3 else parameters(3)

    data.flatMap { x =>
      if (peakMeanDerivativeMetric(testedWord._2, x._2, List(windowSize, deltaX, deltaY)) < proportionSimilarities) {
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
   * @return for each word is returned the a list of peaks represented in a tuple with meaning (PeakIndex, LeftMinValue, RightMinValue)
   */
  def getPeaks(words: RDD[(String, Array[Double])], parameters: List[Double]): RDD[(String, List[(Int, Double, Double)])] = {
    // val proportionSimilarities = parameters.head
    val windowSize = parameters.head.toInt
    val deltaX = parameters(1).toInt
    val deltaY = if (parameters.size < 3) 3 else parameters(2).toInt
    words.map(x => (x._1, testPeak(x._2, windowSize, deltaX, deltaY).map(y => (y._1 + 1840, y._2 + 1840, y._3 + 1840)).sortBy(_._1)))


  }


  /** *******************************************************************************************************
    * Metrics
    * ******************************************************************************************************* */
  /**
   * Compares 2 words and outputs the percent of similarity. Uses windowPeakMinMax as definition for the peaks
   *
   * @param word1 word to be compared
   * @param word2 word to be compared
   * @param windowSize size of the window
   * @param delta tolerated difference between min and max
   * @return
   */
  def peakMaxMinMetric(word1: (String, Array[Double]), word2: (String, Array[Double]), windowSize: Int = 3, delta: Double = -1): Double = {
    peakMMetric(peakMinMax, word1, word2, windowSize, delta)
  }

  /**
   * Compares 2 words and outputs the percent of similarity. Uses windowPeakMean as definition for the peaks
   *
   * @param word1
   * @param word2
   * @param windowSize
   * @param delta
   * @return
   */
  def peakMeanMetric(word1: (String, Array[Double]), word2: (String, Array[Double]), windowSize: Int = 3
                     , delta: Double = -1): Double = {
    peakMMetric(windowPeakMean, word1, word2, windowSize, delta)
  }


  def peakMeanDerivativeMetric(word1: Array[Double], word2: Array[Double], params: List[Double] = List(3, 5, 3)): Double = {
    val windowSize = params.head.toInt
    val deltaX = params(1)
    val deltaY = params(2)
    val distinctPeaks1 = testPeak(word1, windowSize, deltaX, deltaY)
    val distinctPeaks2 = testPeak(word2, windowSize, deltaX, deltaY)

    if (distinctPeaks1.size == 0 || distinctPeaks2.size == 0) {
      return 1.0
    }

    val numberOfSimilarPeaks = (for {
      p1 <- distinctPeaks1
      p2 <- distinctPeaks2
      if Math.abs(p1._1 - p2._1) <= 1
    } yield p1).size

    1 - Math.min(numberOfSimilarPeaks * 1.0 / distinctPeaks1.size, numberOfSimilarPeaks * 1.0 / distinctPeaks2.size)

  }

  def testPeak(word: Array[Double], windowSize: Int, deltaX: Double, deltaY: Double): List[(Int, Double, Double)] = {
    val peaksLeft = peakMeanDerivative(word, windowSize, deltaX, deltaY)
    val peaksRight = peakMeanDerivative(word.reverse, windowSize, deltaX, deltaY)
    val size = word.length

    val peaks = for {
      p1 <- peaksLeft
      p2 <- peaksRight
      if p1._1 == size - p2._1 - 1
    } yield (p1._1, p1._2, size - 1 - p2._2)

    peaks
  }

  /**
   * Helper function that applies the given definition of the peak to the data
   *
   * @param peakDef the definition of the peak to use
   * @param word1 word to compare
   * @param word2 word to compare
   * @param windowSize the size of the window (the smoothing)
   * @param delta the threshold to be used in the peakDefinition
   * @return the similarity value of the two words (between 0 and 1)
   */
  private def peakMMetric(peakDef: ((String, Array[Double]), Int, Double) => List[(Int, Double, Double)],
                          word1: (String, Array[Double]), word2: (String, Array[Double]), windowSize: Int = 3, delta: Double = -1): Double = {
    val deltaUsed = if (delta < 0) {
      Math.max(variance(word1._2), variance(word2._2))
    } else {
      delta
    }

    val distinctPeaks1 = peakDef(word1, windowSize, deltaUsed)
    val distinctPeaks2 = peakDef(word2, windowSize, deltaUsed)

    if (distinctPeaks1.size == 0 || distinctPeaks2.size == 0) {
      return 0.0
    }

    val numberOfSimilarPeaks = (for {
      p1 <- distinctPeaks1
      p2 <- distinctPeaks2
      if p1._1 == p2._1
    } yield p1).size

    Math.min(numberOfSimilarPeaks * 1.0 / distinctPeaks1.size, numberOfSimilarPeaks * 1.0 / distinctPeaks2.size)
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
  //TODO Use windowMaxMin, windowPeakMean
  //TODO implement in a function peakXaxis and peakYaxis
  //TODO implement a method with discrete derivative using the findAscending window and findDescending window: like
  // that we can compare how the function grows with time


  /**
   * For a given word detects the peaks based on looking at all the windows of given size, and the maximum of that
   * window.
   * Then it looks on the right and left side of the maximum and compares the minimums against the maximum. If the
   * difference
   * is larger than the given threshold (delta)
   *
   * @param word who's list we will analyse for peaks
   * @param windowSize the window of years to consider
   * @param delta the threshold: should be the difference on y axis of the words
   * @return List((year, ascending delta, descending delta))
   */
  def peakMinMax(word: (String, Array[Double]), windowSize: Int, delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()

    for (i <- 0 to (frequencies.length - windowSize)) {
      val currentWindow = frequencies.slice(i, windowSize)
      if (currentWindow.nonEmpty) {
        val indexOfMax = currentWindow.indexOf(currentWindow.max)
        val currentLeft = currentWindow.slice(i, indexOfMax)
        val currentRight = currentWindow.slice(indexOfMax, i + windowSize)
        if (currentLeft.nonEmpty && currentRight.nonEmpty && currentWindow(indexOfMax) > delta * currentLeft.min &&
          currentWindow(indexOfMax) > delta * currentRight.min) {
          result = (i + indexOfMax, currentWindow(indexOfMax) - currentLeft.min * 1.0, currentWindow(indexOfMax) -
            currentRight.min * 1.0) :: result
        }
      }
    }
    filterDuplicateYears(result.reverse)
  }

  /**
   * For a given word detects the peaks based on finding the largest ascending window (with some smoothing based on
   * mean), takes the maximum in that window, compares it against the minimum in the ascending window, and then finds
   * the largest descending window starting from the maximum, and once again compares the maximum to the minimum in the
   * window. If the respective differences satisfy the threshold, the maximum is detected as a peak
   *
   * @param word The word which peaks are to be analyzed
   * @param windowSize The size of the smoothing window
   * @param delta the threshold for difference between the minimums and the maximum
   * @return List((year, ascending difference of min and max, descending difference of min and max))
   */
  def windowPeakMean(word: (String, Array[Double]), windowSize: Int, delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()

    var i = 0
    while (i < frequencies.length) {
      val ascendingWindow = frequencies.slice(i, findAscendingAverageWindow(frequencies, i, windowSize) + 1)

      if (ascendingWindow.nonEmpty) {
        val indexOfMaxAscendingInWindow = ascendingWindow.indexOf(ascendingWindow.max)
        val indexOfMaxAscending = indexOfMaxAscendingInWindow + i
        val minAscending = ascendingWindow.slice(0, indexOfMaxAscendingInWindow + 1).min
        if (minAscending != frequencies(indexOfMaxAscending)) {
          val descendingWindow = frequencies
            .slice(indexOfMaxAscending, findDescendingAverageWindow(frequencies, indexOfMaxAscending, windowSize) + 1)
          if (descendingWindow.nonEmpty) {
            val indexOfMinDescending = descendingWindow.indexOf(descendingWindow.min) + indexOfMaxAscending
            val valueAscending = frequencies(indexOfMaxAscending) - minAscending
            val valueDescending = frequencies(indexOfMaxAscending) - frequencies(indexOfMinDescending)
            if (frequencies(indexOfMaxAscending) > minAscending * delta && frequencies(indexOfMaxAscending) > frequencies(indexOfMinDescending) * delta) {
              result = (indexOfMaxAscending, valueAscending * 1.0, valueDescending * 1.0) :: result
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
    filterDuplicateYears(result.reverse)
  }

  /**
   * For a given word detects the peaks based on the maximum descending and ascending windows and taking into
   * consideration the size of those windows. It's different from the windowPeakMean so that instead of outputting
   * the difference between the extrema, it outputs the slope between those points
   *
   * @param word word the size of the smoothing window
   * @param windowSize
   * @param deltaY from 2 to 5
   * @return list of peaks: (Year, left slope, right slope). This list may contain duplicate years and should be
   *         filtered
   *         before any usage
   */

  def peakMeanDerivative(word: Array[Double], windowSize: Int,
                         deltaX: Double = 5, deltaY: Double = 3): List[(Int, Double, Double)] = {
    val frequencies = word
    var result = List[(Int, Double, Double)]()

    var i = 0
    while (i < frequencies.length) {
      val ascendingWindow = frequencies.slice(i, findAscendingAverageWindow(frequencies, i, windowSize) + 1)
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
            .slice(indexOfMaxAscending, findDescendingAverageWindow(frequencies, indexOfMaxAscending, windowSize) + 1)
          //check is the detected window is of size 1
          if (descendingWindow.size > 1) {
            val indexOfMinDescending = descendingWindow.indexOf(descendingWindow.min) + indexOfMaxAscending
            val dyRight = frequencies(indexOfMaxAscending) / frequencies(indexOfMinDescending);
            val dxRight = indexOfMinDescending - indexOfMaxAscending
            if (dyRight * dyRight > dxRight && frequencies(indexOfMaxAscending) > deltaY * frequencies(indexOfMinAscending)) {
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
    filterDuplicateYears(result.reverse)
  }

  /**
   * Finds the maximum index such that the average of the points in a given window size is increasing. Returns that
   * index
   *
   * @param frequencies the array we are considering
   * @param windowSize the number of years we consider in the average
   * @return the position of the last ascending average value
   */
  private def findAscendingAverageWindow(frequencies: Array[Double], start: Int, windowSize: Int): Int = {
    var lastAverage = 0.0
    for (i <- start + 1 to frequencies.length - windowSize) {
      val average = frequencies.slice(start, i).sum * 1.0 / (i - start)
      if (lastAverage <= average) {
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
   * @param windowSize the number of years we consider in the average
   * @return the position of the last descending average value
   */
  private def findDescendingAverageWindow(frequencies: Array[Double], start: Int, windowSize: Int): Int = {
    var lastAverage = frequencies(start)
    for (i <- start + 1 to frequencies.length - windowSize) {
      val average = frequencies.slice(start, i).sum * 1.0 / (i - start)
      if (lastAverage >= average) {
        lastAverage = average
      } else {
        return i - 1
      }
    }
    frequencies.length - 1
  }

}
