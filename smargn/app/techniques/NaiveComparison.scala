package techniques

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import java.io.{File, PrintWriter}

/**
 * Created by Valentin on 17/03/15.
 */
object NaiveComparison {
  def run(word:String, inputDir: String, outputFile: String) = {
    val spark = Spark.ctx

    val data = spark.textFile(inputDir)


    //difference value that is accepted to consider two array values similar
    val acceptedDifference = 3;

    //parse data to have list of value
    val parsedData = data.map(line => line.split(" "))
    //group data in the format (word, Array(list of occurrence))
    val wordOccurrencesData = parsedData.map(i => (i.head, i.tail.toList))
    // Get occurrences of the word
    val wordOccurrencesTuple = wordOccurrencesData.filter {
      case (w, o) => w == word
    }.first
    val wordOccurrences = wordOccurrencesTuple._2
    val similarities = wordOccurrencesData.filter(e => e._1 != word && (e._2.map(_.toInt) zip wordOccurrences.map(_.toInt)).:\(true){
      case ((x, y), acc) => acc && math.abs(x - y) < acceptedDifference
    })
    similarities.map(_._1).saveAsTextFile(outputFile)

    val similaritiesLocal = similarities.collect.toList


//    //add the wordOccurrences to the arrays for future comparison
//    val zipDataTestedWord = wordOccurrencesData.map(x => (x._1, wordOccurrences.zip(x._2)))
//    //test similarity criteria between each data word array and the tested word
//    val booleanDataTestedWord = zipDataTestedWord.map(x => (x._1, x._2.map(x => (x._1.toInt, x._2.toInt)).map(x => math.abs((x._1 - x._2)) < acceptedDifference)))
//    //filter the arrays that have at least one value that didn't pass the similarity test
//    val filterSimilarity = booleanDataTestedWord.map(x => (x._1, x._2.filter(!_))).filter(_._2.length == 0)
//    val similarWords = filterSimilarity.map(x => x._1)
//
//    val completeResult: List[(String, Array[Int])] = filterSimilarity.collect.toList
//    val res = completeResult.map(_._1)
//
//    similarWords.saveAsTextFile(outputFile)

    val firstLine = wordOccurrences.:\("word") {
      case (_, acc) => acc + "," + YearCnt.getNextYear
    }
    val toPrint = firstLine :: (wordOccurrencesTuple :: similaritiesLocal).map {
      case (w, l) => w + l.:\(""){ case (o, acc) => acc + "," + o }
    }
    printToFile(new File(outputFile + "data.csv")) {
      p => toPrint.foreach(p.println)
    }
    similaritiesLocal.map(_._1)
  }

  def printToFile(f: File)(op: PrintWriter => Unit) = {
    val p = new PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}

object YearCnt {
  var year = 2000
  def getNextYear = {
    year = year + 1
    year
  }
}
