package utils

/**
 * Created by Valentin on 15/04/15.
 */
object Grapher {

  def formatForDisplay(startYear: Int, word: (String, Array[Double]), similarities: List[(String, Array[Double])]) = {
    val firstLine = "Word,Year,Occurrences"
    firstLine :: (word :: similarities).flatMap {
      case (w, o) => o.map(_ => w).zip(startYear until (startYear + o.length)).zip(o).map {
        case ((ww, y), oo) => ww + "," + y + "," + oo.toInt
      }
    }
  }

}
