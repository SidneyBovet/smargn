package utils

/**
 * Created by Valentin on 15/04/15.
 */
object Grapher {

  /**
   * Formats the result of the technique into a CSV List[String]
   * Useful for graphing the result with dimple.js
   *
   * @param range the range between the first and the last year of the occurrences
   * @param similar similar word to searched word with its occurrences
   * @return a list of string. Each element corresponds to one unique tuple (word, year, #occurrences in that year)
   */
  def formatTuple(range: Range)(similar: (String, Array[Double])): List[String] = {
    val (w, o) = similar
    o.map(_ => w).zip(range).zip(o).map { case ((ww, y), oo) => ww + "," + y + "," + oo.toInt
    }.toList
  }

  def formatForDisplay(word: List[String], similarWords: Array[String]): List[String] = {
    "Word,Year,Occurrences" :: (word ++ similarWords)
  }
}