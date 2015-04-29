package utils

import org.apache.spark.rdd.RDD
import utils.Smoothing._


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
    println("COunterOFFalse => " + counterOfFalse)
    counterOfFalse
  }

  /**
   * The function is used to found words that are similar up to a time t and then diverge
   * @param zipDataTestedWord list of number of words per year
   * @param convergencePercentage percentage used to define what must be the number of points similar to each other in
   *                              the two tested word in order to say the curve is similar
   *                              (use it with the size of the array of the testedWord)
   * @param differencethreshold threshold used in order to say if two point are similar or not
   *
   * @return if the two tested sample words are similar.
   */
  def evalConvergence(zipDataTestedWord: (String, Array[(Double, Double)]), convergencePercentage: Double, differencethreshold: Double): Boolean = {
    val differenceValue = zipDataTestedWord._2.map(x => math.abs(x._1 - x._2))
    val listOfSimilarPoints = differenceValue.map(y => y <= differencethreshold)
    val numberOfsimilarPoints = listOfSimilarPoints.count(_ == true)
    numberOfsimilarPoints >= zipDataTestedWord._2.length * convergencePercentage
  }


  // Curve MUST be smoothed otherwise it won't work
  def testEvolutionOfDifference(zipDataTestedWord: (String, Array[(Double, Double)]), maxNumberOfOutsidersDecreasing: Int = 7, maxNumberOfOutsidersIncreasing: Int = 7): Boolean = {
    val differences = zipDataTestedWord._2.map(x => math.abs(x._1 - x._2))
    var current: Double = differences.head
    var outsiders = 0
    var outsidersIncreasing = 0
    var x: Double = 0
    var init = true
    var decreasingTesting = true
    val log = true


    for (x <- differences) {
      // must have a leat one "intersection" of the curves
      if (init) {
        current = x
        init = false
      }
      else {

        if (decreasingTesting) {
          if (x < current) {
            println("DOWN")
            current = x
          }
          else {
            println("UP")
            outsiders = outsiders + 1
            if (outsiders >= maxNumberOfOutsidersDecreasing) {
              println("END DECREASING because the curve is not decreasing anymore")
              decreasingTesting = false
            }
          }
        }
        else {
          if (x > current) {
            println("UP")
            current = x
          }
          else {
            println("DOWN")
            outsidersIncreasing = outsidersIncreasing + 1
            if (outsidersIncreasing >= maxNumberOfOutsidersIncreasing) {
              println("BAD END")
              return false
            }
          }
        }

      }


    }
    true
  }

  // In the function the curves will be smoothed!
  def smarterDivergence(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
    val maxNumberOfOutsiders = parameters.head // good to use 7
    val maxNumberOfOutsidersIncreasing = parameters(1) // good to use 7
    val smoothingValue = parameters(2) // good to use >= 10
    val smoothtestedWord = smooth(testedWord, smoothingValue)


    val dataSmoothed = smooth(data, smoothingValue)
    //    println(dataSmoothed.first._2.toList)


    val zipDataTestedWord = dataSmoothed.map(x => (x._1, smoothtestedWord._2.zip(x._2)))
    // take a tuple (word, (point1TestValue1,point1TestValue2),(point2TestValue1,point2TestValue1),...) and first map to (word, true) if they do the divergence
    zipDataTestedWord.map(x => (x._1, testEvolutionOfDifference(x, maxNumberOfOutsiders.toInt, maxNumberOfOutsidersIncreasing.toInt))).filter(x => x._2).map(_._1)
  }


  //  //  WORK IN PROGRESS
  //  def testConvergence(zipDataTestedWord: (String, Array[(Double, Double)]), minimumConvergenceWindow: Double = 7.0): Boolean = {
  //    val arrToTest = zipDataTestedWord._2
  //    var middleIndex: Int = arrToTest.length / 2
  //    var initialIndexAddLeft: Int = minimumConvergenceWindow.toInt / 2
  //    var initialIndexAddRight: Int = minimumConvergenceWindow.toInt - initialIndexAddLeft
  //    var rightIndex: Int = 0
  //    var leftIndex: Int = 0
  //    if (minimumConvergenceWindow > 1.0) {
  //      rightIndex = middleIndex + initialIndexAddRight
  //      leftIndex = middleIndex - initialIndexAddLeft
  //    }
  //    else {
  //      rightIndex = middleIndex
  //      leftIndex = middleIndex - 1
  //    }
  //    true
  //  }


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