package utils

import org.apache.spark.rdd.RDD


/**
 * Created by johngaspoz and Fabien on 15.04.2015.
 *
 */
object SubTechniques {

  /**
   * Given a word, returns the same word, but with a reversed curve
   * @param testedWord word which curve we want to reverse
   * @return word with a reversed curve
   */
  def inverse(testedWord: (String, Array[Double])): (String, Array[Double]) = {
    (testedWord._1, testedWord._2.reverse)
  }

  /**
   * Tries to match the given word by shifting it before applying the given technique.
   * The function will try to shift the data to the right and to the left to match the given word.
   * The total number of times the technique is applied will be (2 * shiftLen) + 1
   * (The function will always start by trying to match the word as-is, without shifting it, even if shiftLen=0)
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param parameters parameters applicable to the given technique
   * @param similarityTechnique the function that implements the technique we want to use
   * @param shiftLen how many step we will apply the technique to BOTH side
   * @param shiftStep the size of a single shift
   *
   * @return words that are similar but shifted (right or left)
   */
  def shift(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double],
            similarityTechnique: (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[(String)],
            shiftLen: Int, shiftStep: Int): RDD[(String)] = {

    var unionWord: RDD[(String)] = similarityTechnique(data, testedWord, parameters)

    var testedWordReduced: (String, Array[Double]) = testedWord
    var dataReduced: RDD[(String, Array[Double])] = data

    //word to the right, data to the left
    for (i <- 0 until shiftLen by shiftStep) {
      testedWordReduced = (testedWord._1, testedWordReduced._2.dropRight(shiftStep))
      dataReduced = dataReduced.map(x => (x._1, x._2.drop(shiftStep)))

      unionWord = unionWord.union(similarityTechnique(dataReduced, testedWordReduced, parameters)).distinct()
    }

    testedWordReduced = testedWord
    dataReduced = data

    //word to the left, data to the right
    for (i <- 0 until shiftLen by shiftStep) {
      testedWordReduced = (testedWord._1, testedWordReduced._2.drop(shiftStep))
      dataReduced = dataReduced.map(x => (x._1, x._2.dropRight(shiftStep)))

      unionWord = unionWord.union(similarityTechnique(dataReduced, testedWordReduced, parameters)).distinct()
    }


    unionWord
  }
}