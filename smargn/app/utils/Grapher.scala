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
   * @param word the word we search together with its occurrences
   * @param similarities all similar words to word with their occurrences
   * @return a list of string. Each element corresponds to one unique tuple (word, year, #occurrences in that year)
   */
  def formatForDisplay(range: Range, word: (String, Array[Double]),
                       similarities: List[(String, Array[Double])]): List[String] = {
    val firstLine = "Word,Year,Occurrences"
    firstLine :: (word :: similarities).flatMap
    { case (w, o) => o.map(_ => w).zip(range).zip(o).map { case ((ww, y), oo) => ww + "," + y + "," + oo.toInt
    }
    }
  }

}