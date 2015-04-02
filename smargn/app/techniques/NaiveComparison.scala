package techniques

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import play.Logger

/**
 * Created by Valentin on 17/03/15.
 * Updated by Joanna (main code) on 26/03/15.
 */
object NaiveComparison {

  def run(word: String, inputDir: String, outputFile: String): List[String] = {
    val spark = Spark.ctx
    Logger.info("Searching for word: " + word)

    val data = spark.textFile(inputDir)


    //difference value that is accepted to consider two array values similar
    val target = new File(outputFile)
    if (target.exists()) {
      Logger.info("Deleting previous output folder")
      deleteFolder(target)
    }

    val formattedData = data.map(line => line.split(" "))
      .map((i: Array[String]) => (i.head, i.tail.map(y => y.toDouble)))


    //difference value that is accepted to consider two array values similar
    //    val acceptedDifferenceNaiveDifference = 3
    //    val acceptedDifferenceNaiveDifferenceMaxScale = 0.2
    //    val acceptedDifferenceNaiveDifferenceAvgScale = 0.2
    val acceptedDifferenceNaiveDivision = 0.5


    //TODO deal with word not find in data
    //get word that we want to have its similar words from run's argument and find it in the data
    val listWords = formattedData.filter { case (w, o) => w == word
    }
    if (listWords.count() == 0) {
      Logger.debug(word + " is not in the data")
      Nil
    } else {
      val testedWord = listWords.first()

      //TODO find a way to choose function
      //      val similarWordsDifference = naiveDifference(formattedData, testedWord, acceptedDifferenceNaiveDifference)
      //      val similarWordsDifferenceMax = naiveDifference(formattedData.map(proportionalScalarMax(_)),
      // proportionalScalarMax(testedWord), acceptedDifferenceNaiveDifferenceMaxScale)
      //      val similarWordsDifferenceAverage = naiveDifference(formattedData.map(proportionalScalarAverage(_)),
      // proportionalScalarAverage(testedWord), acceptedDifferenceNaiveDifferenceAvgScale)
      val similarWordsDivision = naiveDivision(formattedData, testedWord, acceptedDifferenceNaiveDivision)

      similarWordsDivision.map(_._1).saveAsTextFile(outputFile)

      val similaritiesLocal: List[(String, Array[Double])] = similarWordsDivision.collect().toList

      //TODO finish display graph
      val startYear = 2000
      val firstLine = "Word,Year,Occurrences"

      val toPrint = firstLine :: (testedWord :: similaritiesLocal).flatMap {
        case (w, o) => o.map(_ => w).zip(startYear until (startYear + o.length)).zip(o).map {
          case ((ww, y), oo) => ww + "," + y + "," + oo.toInt
        }
      }
      printToFile(new File(outputFile + "data.csv")) {
        p => toPrint.foreach(p.println)

          Logger.info("Found " + similarWordsDivision.count() + " similar words")
          similaritiesLocal.map(_._1)
      }
      similaritiesLocal.unzip._1
    }
  }

  def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles
    files.foreach(f => {
      if (f.isDirectory) deleteFolder(f) else f.delete
    })
    folder.delete
  }

  def printToFile(f: File)(op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  /**
   * Find the min and max of an array
   * @param array array we want to find its min and max
   * @return tuple of min and max of the array
   */
  //taken from http://stackoverflow.com/questions/20285209/find-min-and-max-elements-of-array
  def findMinAndMax(array: Array[Double]): (Double, Double) = {
    // a non-empty array
    if (array.length != 0) {
      val initial = (array.head, array.head) // a tuple representing min-max
      array.foldLeft(initial) { (acc, x) => if (x < acc._1) (x, acc._2) else if (x > acc._2) (acc._1, x) else acc }
    } else {
      (-1.0, -1.0)
    }
  }

  /**
   * Scale the word, frequency tuple with the help of its maximum frequency
   * @param wordFreq tuple of word and its frequency
   * @return tuple of word and its max-scaled frequency
   */
  def proportionalScalarMax(wordFreq: (String, Array[Double])): (String, Array[Double]) = {
    val max = findMinAndMax(wordFreq._2)._2
    (wordFreq._1, wordFreq._2.map(x => x / max))
  }

  /**
   * Scale the word, frequency tuple with the help of its average frequency
   * @param formattedWord tuple of word and its frequency
   * @return tuple of word and its average-scaled frequency
   */
  def proportionalScalarAverage(formattedWord: (String, Array[Double])): (String, Array[Double]) = {
    val average = formattedWord._2.sum
    (formattedWord._1, formattedWord._2.map(x => x / average))
  }

  /**
   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by computing
   * array's elements difference
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param acceptedDifference difference between two array value that we accept
   * @return words that are similar
   */
  def naiveDifference(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                      acceptedDifference: Double): RDD[(String, Array[Double])] = {
    //add the testedWord values to the arrays and compute difference for future comparison
    val zipDataTestedWord = data.map(x => (x._1, testedWord._2.zip(x._2).map(x => math.abs(x._1 - x._2)), x._2))
    //test similarity criteria between each data word array and the tested word
    val booleanDataTestedWord = zipDataTestedWord.map(x => (x._1, x._2.map(y => y <= acceptedDifference), x._3))
    //filter the arrays that have at least one value that didn't pass the similarity test
    booleanDataTestedWord.map(x => (x._1, x._2.filter(_ == false), x._3))
      .filter(x => x._2.length == 0 && x._1 != testedWord._1).map(x => (x._1, x._3))
  }

  /**
   * Compare the ratio of word's frequency with the collection word's frequency to find similar words by computing
   * the ratio line
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param acceptedDifference straightness of the curve that we accept
   * @return words that are similar
   */
  def naiveDivision(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                    acceptedDifference: Double): RDD[(String, Array[Double])] = {
    //add the testedWord values to the arrays and compute division for future comparison
    val dividedDataTestedWord = data.map(x => (x._1, testedWord._2.zip(x._2).map(x => x._1 / x._2), x._2))
    //could be useful for testing purpose
    //val tempDifferenceMaxMin = dividedDataTestedWord.map(x => (x._1, (((findMinAndMax(x._2))._2)- ((findMinAndMax(x
    // ._2))._1))))
    val booleanDataTestedWord = dividedDataTestedWord
      .map(x => (x._1, findMinAndMax(x._2)._2 - findMinAndMax(x._2)._1 < acceptedDifference, x._3))
    booleanDataTestedWord.filter(x => x._2 && x._1 != testedWord._1).map(x => (x._1, x._3))
  }
}
