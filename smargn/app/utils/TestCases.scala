package utils

import org.apache.spark.rdd.RDD

/**
 * Created by fabien on 4/28/15.
 */
object TestCases {


  def count(result : RDD[(String)],  wordList: List[String]) : Int = {
    var value = 0
    wordList.foreach(
      word => result.foreach(
        x => if (x == word) {
          value += 1
        }
      )
    )
    value
  }

  def test(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), similarWords: List[String],
           differentWords: List[String], parameters: List[Double],
           similarityTechnique: (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[(String)])
            : Double = {

    var result : RDD[(String)] = similarityTechnique(data,testedWord,parameters)


    var simWords = count(result,similarWords)
    var diffWords = count(result,differentWords)
    var simRatio = simWords.toDouble / similarWords.size.toDouble
    var diffRatio = diffWords.toDouble / similarWords.size.toDouble




    (simRatio + (1-diffRatio))/2
  }
}
