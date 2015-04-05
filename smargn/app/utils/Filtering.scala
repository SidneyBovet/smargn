package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by sidney on 05.04.15.
 */
object Filtering {
  /**
   *
   * @param fileName The path to the CSV file using space as delimiters to be parsed
   * @param sc The SparkContext to run in
   * @return an RDD[String,Array[Int] ] (word,[occInYear0,occInYear1,...])
   */
  def filenameToKeyValue(fileName: String, sc: SparkContext) = sc.textFile(fileName)
    .map(_.split(" "))
    .keyBy(_.head) // produce (word,[w,f1,f2,...]) tuples
    .map(k => (k._1,k._2.tail.map(_.toInt)))

  /**
   *
   * @param a The array to compute the mean of
   * @return The mean of the array a
   */
  def mean(a: Array[Int]): Double = {
    val sum = a.foldLeft(0)(_+_)
    sum/a.size
  }

  /**
   *
   * @param array the temporal profile of a word
   * @param mean the mean value of the array, 1 if the array was previously divided by its mean
   * @param threshold the threshold value to discriminate normal points
   * @return true if any point in array goes too far from the mean
   */
  def detectPeaks(array: Array[Double], mean: Double, threshold: Double): Boolean = {
    array.foldLeft(false)((b:Boolean,el:Double) => {
      (b || ((el-mean)*(el-mean) > threshold))
    })
  }

  /**
   *
   * @param inputFile File to read the profiles from.
   * @param sc SparkContext attached to the application.
   * @param threshold A positive threshold value. The lower the more words are output.
   * @return A list of words having unusually small or big values during one or more year(s).
   */
  def interestingWords(inputFile: String, sc: SparkContext, threshold: Double): RDD[String] = {
    val kvPairs = filenameToKeyValue(inputFile,sc).cache
    val kvPairsNorm = kvPairs.map(t => {
      val meanValue = mean(t._2)
      (t._1, t._2.map(_ / meanValue))
    })
    kvPairsNorm.flatMap(t => {
      if(detectPeaks(t._2,1,1)) {
        List(t._1)
      } else {
        List()
      }
    })
  }

}
