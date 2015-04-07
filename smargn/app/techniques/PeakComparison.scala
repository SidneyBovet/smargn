package techniques

import java.io.File
import org.apache.spark.rdd.RDD
import utils.ComputationUtilities._

import play.Logger


object PeakComparison {

  val deltaMinMax = 0
  val average = 0
  //call average function
  val variance = 0
  //call variance function
  val deltaSlope = 1

  /**
   *
   * @param data
   * @param testedWord
   * @param parameters The head of the list is the window size
   * @return
   */
  def peakComparisonWithDerivative(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val windowSize = parameters(0)
    val deltaSlope = 1
    val axisXDeviation = parameters(1)
    val proportionSimilarities = parameters(2)
    val peakTestedWord = windowPeakDerivative(testedWord, windowSize.toInt, deltaSlope)

    val peaksOfWords = data.map(x => (x._1, windowPeakDerivative(x, windowSize.toInt, deltaSlope)))

    val maxIndexesOfWords: RDD[(String, List[Int])] = peaksOfWords.map(x => (x._1, x._2.map(y => y._1)))
    val maxIndexesOfWord: List[Int] = peakTestedWord.map(x => x._1)
    //peak x axis
    val proportionPeakSimilarities = maxIndexesOfWords.map(x => (x._1, x._2.map(y => maxIndexesOfWord.map(z => Math.abs(z - y) < axisXDeviation).count(_ == true) > 0))).map(x => (x._1, x._2.count(_ == true) / maxIndexesOfWord.size))
    proportionPeakSimilarities.filter(_._2 > proportionSimilarities).map(_._1)
  }
  
  //TODO Use windowMaxMin, windowPeakMean
  //TODO implement in a function peakXaxis and peakYaxis

  /**
   *
   * @param word who's list we will analyse for peaks
   * @param windowSize the window of years to consider
   * @param delta the threshold: should be a slope i.e. 1 is for angle of 45degrees
   * @return List((year, ascending slope, descending slope))
   */
  private def windowPeakDerivative(word: (String, Array[Double]), windowSize: Int, delta: Double): List[(Int, Double, Double)] = {
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
   *
   * @param word who's list we will analyse for peaks
   * @param windowSize the window of years to consider
   * @param delta the threshold: should be the difference on y axis of the words
   * @return List((year, ascending slope, descending slope))
   */
  private def windowPeakMinMax(word: (String, Array[Double]), windowSize: Int, delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()
    for (i <- 0 to (frequencies.length - windowSize)) {
      val currentWindow = frequencies.slice(i, windowSize)
      val indexOfMax = currentWindow.indexOf(currentWindow.max)
      val currentLeft = currentWindow.slice(i, indexOfMax)
      val currentRight = currentWindow.slice(indexOfMax, i + windowSize)
      if (currentWindow(indexOfMax) - currentLeft.min > delta && currentWindow(indexOfMax) - currentRight.min > delta) {
        result = (i + indexOfMax, currentWindow(indexOfMax) - currentLeft.min * 1.0, currentWindow(indexOfMax) - currentRight.min * 1.0) :: result
      }
    }
    result.reverse
  }

  private def windowPeakMean(word: (String, Array[Double]), windowSize: Int, delta: Double): List[(Int, Double, Double)] = {
    val frequencies = word._2
    var result = List[(Int, Double, Double)]()

    var i = 0
    while (i < frequencies.length) {
      val ascendingWindow = frequencies.slice(i, findAscendingAverageWindow(frequencies, i, windowSize) + 1)
      val indexOfMaxAscendingInWindow = ascendingWindow.indexOf(ascendingWindow.max)
      val indexOfMaxAscending = indexOfMaxAscendingInWindow + i
      val minAscending = ascendingWindow.slice(0, indexOfMaxAscendingInWindow + 1).min
      val descendingWindow = frequencies.slice(indexOfMaxAscending, findDescendingAverageWindow(frequencies, indexOfMaxAscending, windowSize) + 1)
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
      }
      else {
        return i - 1
      }
    }
    frequencies.length - 1
  }

  /**
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
      }
      else {
        return i - 1
      }
    }
    frequencies.length - 1
  }

  /**
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
   *
   * @param window
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

  private def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles
    files.foreach(f => {
      if (f.isDirectory) deleteFolder(f) else f.delete
    })
    folder.delete
  }


}
