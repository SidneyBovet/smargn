package utils

import org.apache.spark.rdd.RDD

/**
 * Created by Joanna on 4/7/15.
 * Formatting functions to parse the words
 */
object Formatting {

  val summedWordPerYear = Array(2122323, 2244681, 2090514, 2065231, 1977265, 2024515, 1930356, 2277546, 1999820,
    2101924, 2697558, 3219945, 3159865, 3001991, 3400445, 3424123, 4755786, 4865075, 4890963, 4452536, 4757160, 4888676,
    5692283, 6336701, 6595610, 5955054, 5799465, 5856907, 6429359, 6592246, 7391701, 7861885, 8147593, 8025362, 7718126,
    8325643, 6853073, 8364891, 8396568, 8230138, 8355511, 8645025, 8120658, 8517314, 8198095, 7992581, 7885120, 8209978,
    8176737, 8018662, 8109653, 9393429, 9391806, 9849588, 9820476, 9699829, 10320342, 10109845, 10712237, 11274764,
    11262049, 11405377, 11331632, 11187626, 10673355, 10257862, 10730687, 11474657, 11289690, 11224426, 11931212,
    11809556, 12166655, 12845947, 11197905, 10440220, 11512458, 4769483, 4131755, 4767862, 8276512, 10789993, 11370154,
    12221120, 11976838, 12744341, 13118419, 12679902, 12830140, 13786786, 14661115, 13426685, 13290765, 13276314,
    13641513, 13557697, 10497287, 13805143, 12954176, 11950092, 9769621, 10601749, 12018854, 11715157, 12542519,
    11883999, 13560945, 12045927, 13016994, 13299083, 13498171, 12699478, 13179157, 13659607, 14406374, 14006581,
    13517493, 14822764, 14638583, 14626616, 16248005, 16847006, 16681617, 16555457, 18025636, 15627347, 18112566,
    16430876, 14775628, 16012264, 16658242, 14676114, 13569468, 15397707, 14594267, 14970719, 14062956, 13471200,
    18348500, 17082730, 16321588, 17267341, 19114344, 18872176, 20156965, 21053925, 45482968, 49240500, 47060830,
    20575469, 20826940, 84016140, 185138460, 49190342, 79628120, 79902604, 130423848, 182917206, 4967354);

  /**
   * Format the data
   * @param data data to be formatted
   * @return the formatted representation (word, freq) of the data
   */
  def dataFormatter(data: RDD[(String)]): RDD[(String, Array[Double])] = {
    data.map(line => line.split("\\s")).map((i: Array[String]) => (i.head, i.tail.map(y => y.toDouble)))
  }

  def dataFormatter2(data: RDD[(String)]): RDD[(String, Array[Double])] = {
    data.map(line => line.split("\\s")).map(
      (i: Array[String]) => (i.head, i.tail.map(y => y.toDouble).zip(summedWordPerYear).map(x => x._1 / x._2.toDouble)))

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

}
