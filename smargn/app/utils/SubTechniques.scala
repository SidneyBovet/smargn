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

  /**
   * Tries to match the given word by shifting it before applying the given technique.
   * The function will try to shift the data to the right and to the left to match the given word.
   * The total number of times the technique is applied will be (2 * shiftLen) + 1
   * (The function will always start by trying to match the word as-is, without shifting it, even if shiftLen=0)
   * @param arr array of false of true value
   * @return the number of false value found in the array starting for the end
   */
  def numOfFalseFromEnd(arr: Array[Boolean], limit: Double): Int = {
    var notExcepted = 0
    var counterOfFalse = 0
    for (i <- (arr.length - 1) to 0 by -1) {
      if (!arr(i)) {
        counterOfFalse = counterOfFalse + 1
      }
      else {
        notExcepted = notExcepted + 1
        if (notExcepted >= limit) {
          return counterOfFalse
        }
      }
    }
    counterOfFalse
  }

  /**
   * The function is used to found words that are similar up to a time t and then diverge
   * @param testedWordSample list of number of words per year
   * @param AWordSampleFromData list of number of words per year
   * @param convergencePercentage percentage used to define what must be the number of points similar to each other in
   *                              the two tested word in order to say the curve is similar
   *                              (use it with the size of the array of the testedWord)
   * @param differencethreshold threshold used in order to say if two point are similar or not
   *
   * @return if the two tested sample words are similar.
   */
  def evalConvergence(testedWordSample: Array[Double], AWordSampleFromData: Array[Double], convergencePercentage: Double, differencethreshold: Double): Boolean = {
    val differenceValue = testedWordSample.zip(AWordSampleFromData).map(x => math.abs(x._1 - x._2))
    val listOfSimilarPoints = differenceValue.map(y => y <= differencethreshold)
    val numberOfsimilarPoints = listOfSimilarPoints.filter(_ == true).size
    return numberOfsimilarPoints >= testedWordSample.length * convergencePercentage
  }

  /**
   * The function is used to found words that are similar up to a time t and then diverge
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word (acceptedDifference, miniOfDivergence, falseVariation)
   * @param parameters parameters applicable to the given technique
   * @return words that are similar up to a time t and then diverge
   */
  def divergence(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val acceptedDifference = parameters.head
    //use miniOfDivergence =  30
    val miniOfDivergence = parameters(1)
    val falseVariation = parameters(2)
    //add the testedWord values to the arrays and compute difference for future comparison
    val zipDataTestedWord = data.map(x => (x._1, testedWord._2.zip(x._2).map(x => math.abs(x._1 - x._2)), x._2))
    //test similarity criteria between each data word array and the tested word
    val booleanDataTestedWord = zipDataTestedWord.map(x => (x._1, x._2.map(y => y <= acceptedDifference)))
    //filter the arrays that have at least one value that didn't pass the similarity test

    //TODO REMVOE THE numb of False values accepted Value TO A DYNAMIC ONE
    // if numOfFalseFromEnd(x._2, falseVariation) >= miniOfDivergence is fasle then the curve do not really diverge
    booleanDataTestedWord.map(x => (x._1, x._2.filter(_ == false)))
      .filter(x => x._2.length <= 2 * miniOfDivergence && x._1 != testedWord._1 && numOfFalseFromEnd(x._2, falseVariation) >= miniOfDivergence).map(_._1)
  }
}