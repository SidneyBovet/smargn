package techniques

import java.io.File

import org.apache.spark.rdd.RDD
import play.Logger


object PeakComparison {
  def run(word: String, inputDir: String, outputFile: String): List[String] = {
    //Configs
    val spark = Spark.ctx
    Logger.info("Searching for word: " + word)

    val data = spark.textFile(inputDir)
    val formattedData = data.map(line => line.split(" "))
      .map((i: Array[String]) => (i.head, i.tail.map(y => y.toDouble)))


    //difference value that is accepted to consider two array values similar
    val target = new File(outputFile)
    if (target.exists()) {
      Logger.info("Deleting previous output folder")
      deleteFolder(target)
    }

    val deltaMinMax = 0 //call minmax function
    val average = 0 //call average function
    val variance = 0 //call variance function
    val deltaSlope = 1




    List("")
  }

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

      lastDerivative = derivative
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
    result
  }

 
  /**
   *
   * @param window the array we are considering
   * @param windowSize the number of years we consider in the average
   * @return the position of the last ascending average value
   */
  private def findAscendingAverageWindow(window: Array[Double], windowSize: Int): Int = {
    var lastAverage = 0.0
    for (i <- 0 to window.length - windowSize) {
      val average = window.slice(0, i).sum / (i + 1.0)
      if (lastAverage <= average) {
        lastAverage = average
      }
      else {
        return i - 1
      }
    }
    window.length - 1
  }

  /**
   *
   * @param window the array we are considering
   * @param windowSize the number of years we consider in the average
   * @return the position of the last descending average value
   */
  private def findDescendingAverageWindow(window: Array[Double], windowSize: Int): Int = {
    var lastAverage = 0.0
    for (i <- 0 to window.length - windowSize) {
      val average = window.slice(0, i).sum / (i + 1.0)
      if (lastAverage >= average) {
        lastAverage = average
      }
      else {
        return i - 1
      }
    }
    window.length - 1
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
