package utils

import org.apache.spark.rdd.RDD
import utils.Launcher.Technique

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
           similarityTechnique: Technique): Double = {

    val result : RDD[(String)] = similarityTechnique(data,testedWord,parameters)


    val simWords = count(result,similarWords)
    val diffWords = count(result,differentWords)
    val simRatio = simWords.toDouble / similarWords.size.toDouble
    val diffRatio = diffWords.toDouble / similarWords.size.toDouble




    (simRatio + (1-diffRatio))/2
  }
}
