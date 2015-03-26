package techniques

import java.io.{File, PrintWriter}

import play.Logger

/**
 * Created by Valentin on 17/03/15.
 */
object NaiveComparison {
  def run(word:String, inputDir: String, outputFile: String) = {
//    val spark = new SparkContext(new SparkConf().setAppName("naiveCompare").setMaster("local[2]"))
//    //                          .setMaster("yarn-client")
    val spark = Spark.ctx
    Logger.info("Searching for word: " + word)

    val data = spark.textFile(inputDir)


    //difference value that is accepted to consider two array values similar
    val acceptedDifference = 3
    val target = new File(outputFile)
    if(target.exists()) {
      Logger.info("Deleting previous output folder")
      deleteFolder(target)
    }

    //parse data to have list of value
    val parsedData = data.map(line => line.split(" "))
    //group data in the format (word, Array(list of occurrence))
    val wordOccurrencesData = parsedData.map(i => (i.head, i.tail.toList))
    // Get occurrences of the word
    val listWords = wordOccurrencesData.filter {
      case (w, o) => w == word
    }
    if(listWords.count() == 0) {
      Logger.debug(word + " is not in the data")
      Nil
    } else {
      val wordOccurrencesTuple = listWords.first
      val wordOccurrences = wordOccurrencesTuple._2
      val similarities = wordOccurrencesData.filter(e => e._1 != word && (e._2.map(_.toInt) zip wordOccurrences.map(_.toInt)).:\(true){
        case ((x, y), acc) => acc && math.abs(x - y) < acceptedDifference
      })
      similarities.map(_._1).saveAsTextFile(outputFile)

      val similaritiesLocal = similarities.collect.toList

      val firstLine = wordOccurrences.:\("word") {
        case (_, acc) => acc + "," + YearCnt.getNextYear
      }
      val toPrint = firstLine :: (wordOccurrencesTuple :: similaritiesLocal).map {
        case (w, l) => w + l.:\(""){ case (o, acc) => acc + "," + o }
      }
      printToFile(new File(outputFile + "data.csv")) {
        p => toPrint.foreach(p.println)
      }
      Logger.info("Found " + similarities.count() + " similar words")
      similaritiesLocal.map(_._1)
    }
  }

  def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles
    files.foreach(f => {
      if(f.isDirectory) {
        deleteFolder(f)
      } else {
        f.delete
      }
    })
    folder.delete
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
  var year = 1999
  def getNextYear = {
    year = year + 1
    year
  }
}
