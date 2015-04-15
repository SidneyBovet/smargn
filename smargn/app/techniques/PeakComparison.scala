package techniques

import org.apache.spark.rdd.RDD


object PeakComparison {

  val deltaMinMax = 0
  val average = 0
  //call average function
  val variance = 0
  //call variance function
  val deltaSlope = 1

  /**
   * Compare the given word against the data based on the peaks. Here peak is defined as having a slope larger than 1.
   * The slope is calculated with discrete derivative between 2 points separated by a given window of time
   *
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters L(0) contains the window between 2 points which discrete derivative we calculate
   * @return words that are similar
   */
  def peakComparisonWithDerivative(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                                   parameters: List[Double]): RDD[(String)] = {
    val windowSize = parameters.head
    val deltaSlope = 1
    val axisXDeviation = parameters(1)
    val proportionSimilarities = parameters(2)
    val peakTestedWord = windowPeakDerivative(testedWord, windowSize.toInt, deltaSlope)

    val peaksOfWords = data.map(x => (x._1, windowPeakDerivative(x, windowSize.toInt, deltaSlope)))

    val maxIndexesOfWords: RDD[(String, List[Int])] = peaksOfWords.map(x => (x._1, x._2.map(y => y._1)))
    val maxIndexesOfWord: List[Int] = peakTestedWord.map(x => x._1)
    //peak x axis
    val proportionPeakSimilarities = maxIndexesOfWords
      .map(x => (x._1, x._2.map(y => maxIndexesOfWord.map(z => Math.abs(z - y) < axisXDeviation).count(_ == true) > 0)))
      .map(x => (x._1, x._2.count(_ == true) / maxIndexesOfWord.size))
    proportionPeakSimilarities.filter(_._2 > proportionSimilarities).map(_._1)
  }

  //TODO Use windowMaxMin, windowPeakMean
  //TODO implement in a function peakXaxis and peakYaxis
  //TODO implement a method with discrete derivative using the findAscending window and findDescending window: like
  // that we can compare how the function grows with time
  /**
   * For a given word detects the peaks based on discrete derivative
   *
   * @param word who's list we will analyse for peaks
   * @param windowSize the window of years to consider
   * @param delta the threshold: should be a slope i.e. 1 is for angle of 45degrees
   * @return List((year, ascending slope, descending slope))
   */
  private def windowPeakDerivative(word: (String, Array[Double]), windowSize: Int,
                                   delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()
    var lastDerivative = 0.0

    for (i <- 0 until (frequencies.length - windowSize)) {
      val derivative = (frequencies(i + windowSize) - frequencies(i)) / (windowSize * 1.0)
      if (lastDerivative > delta && derivative < -delta) {
        result = (i + windowSize, lastDerivative, derivative) :: result
      }
      if (derivative * lastDerivative > 0) {
        lastDerivative = derivative
      }
    }
    result.reverse
  }

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
   * @return List((year, ascending slope, descending slope))
   */
  private def windowPeakMinMax(word: (String, Array[Double]), windowSize: Int,
                               delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()
    for (i <- 0 to (frequencies.length - windowSize)) {
      val currentWindow = frequencies.slice(i, windowSize)
      val indexOfMax = currentWindow.indexOf(currentWindow.max)
      val currentLeft = currentWindow.slice(i, indexOfMax)
      val currentRight = currentWindow.slice(indexOfMax, i + windowSize)
      if (currentWindow(indexOfMax) - currentLeft.min > delta && currentWindow(indexOfMax) - currentRight.min > delta) {
        result = (i + indexOfMax, currentWindow(indexOfMax) - currentLeft.min * 1.0, currentWindow(indexOfMax) -
          currentRight.min * 1.0) :: result
      }
    }
    result.reverse
      .distinct //TODO ensure that each year is present only once in the list with the max slopes encountered
  }

  /**
   * For a given word detects the peaks based on finding the largest ascending window (with some smoothing based on
   * mean), takes
   * the maximum in that window, compares it against the minimum in the ascending window, and then finds the largest
   * descending
   * window starting from the maximum, and once again compares the maximum to the minimum in the window. If the
   * respective
   * differences satisfy the threshold, the maximum is detected as a peak
   *
   * @param word The word which peaks are to be analyzed
   * @param windowSize The size of the smoothing window
   * @param delta the threshold for difference between the minimums and the maximum
   * @return List((year, ascending difference of min and max, descending difference of min and max))
   */
  private def windowPeakMean(word: (String, Array[Double]), windowSize: Int,
                             delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()

    var i = 0
    while (i < frequencies.length) {
      val ascendingWindow = frequencies.slice(i, findAscendingAverageWindow(frequencies, i, windowSize) + 1)
      val indexOfMaxAscendingInWindow = ascendingWindow.indexOf(ascendingWindow.max)
      val indexOfMaxAscending = indexOfMaxAscendingInWindow + i
      val minAscending = ascendingWindow.slice(0, indexOfMaxAscendingInWindow + 1).min
      val descendingWindow = frequencies
        .slice(indexOfMaxAscending, findDescendingAverageWindow(frequencies, indexOfMaxAscending, windowSize) + 1)
      val indexOfMinDescending = descendingWindow.indexOf(descendingWindow.min) + indexOfMaxAscending
      val valueAscending = frequencies(indexOfMaxAscending) - minAscending
      val valueDescending = frequencies(indexOfMaxAscending) - frequencies(indexOfMinDescending)
      if (valueAscending > delta && valueDescending > delta) {
        result = (indexOfMaxAscending, valueAscending * 1.0, valueDescending * 1.0) :: result
      }
      i = indexOfMinDescending
    }
    result.reverse
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
    for (i <- start to frequencies.length - windowSize) {
      val average = frequencies.slice(0, i).sum / (i + 1.0)
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
    var lastAverage = 0.0
    for (i <- start to frequencies.length - windowSize) {
      val average = frequencies.slice(0, i).sum / (i + 1.0)
      if (lastAverage >= average) {
        lastAverage = average
      } else {
        return i - 1
      }
    }
    frequencies.length - 1
  }

  /**
   * Finds the maximum index such that the points in the window are strictly increasing
   *
   * @param window array we are considering
   * @return the position of the last ascending value
   */
  private def findAscendingWindow(window: Array[Double]): Int = {
    for (i <- 0 until window.length - 1) {
      if (window(i) >= window(i + 1)) {
        return i
      }
    }
    window.length - 1
  }

  /**
   * Finds the maximum index such that the points in the window are strictly decreasing
   *
   * @param window array we are considering
   * @return the position of the last descending value
   */
  private def findDescendingWindow(window: Array[Double]): Int = {
    for (i <- 0 until window.length - 1) {
      if (window(i) <= window(i + 1)) {
        return i
      }
    }
    window.length - 1
  }

}
