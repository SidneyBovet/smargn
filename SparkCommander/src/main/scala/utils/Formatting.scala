package utils

import org.apache.spark.rdd.RDD

/**
 * Created by Joanna on 4/7/15.
 * Formatting functions to parse the words
 */
object Formatting {
  /**
   * Format the data
   * @param data data to be formatted
   * @return the formatted representation (word, freq) of the data
   */
  def dataFormatter(data: RDD[(String)], baseProfile: Array[Int]): RDD[(String, Array[Double])] = {
    data.map(line => line.split("\\s")).map(
      (i: Array[String]) => (i.head, i.tail.map(y => y.toDouble).zip(baseProfile).map(x => x._1 / x._2.toDouble)))
  }

  /**
   * Get the word's frequency from the data for a list of words
   * @param formattedData the formatted data we will look into
   * @param words the list of words we want to find the frequency
   * @return the complete representation (word, freq) of the list of words
   */
  def searchWordFormatter(formattedData: RDD[(String, Array[Double])],
                          words: List[String]): RDD[(String, Array[Double])] = {
    formattedData.filter { case (w, o) => words.contains(w)
    }
  }

  private def slice[T](l: Array[T], range: Range): Array[T] = {
    val t: Array[T] = l.zip(Launcher.startYear to Launcher.endYear).filter(y => range.contains(y._2)).map { case (occ, r) => occ }
    t
  }
}
