//package techniques
//
//import org.apache.spark.rdd.RDD
//import utils.Scaling._
//
///**
// * Created by johngaspoz on 12.04.15
// * Naive functions that compute the similar words list of a given word based on comparison techniques
// */
//
//object NaiveShiftComparison {
//
//  import utils.ComputationUtilities._
//
//
//  def shiftList(sens: String, diff: Int, list: List[Double]): List[Double] = {
//    var shiftedFull = List()
//    if (sens == "left") {
//      val shifted = list.drop(diff)
//    }
//    else {
//      val shifted = list.dropRight(diff)
//    }
//    println(shiftedFull)
//    shiftedFull
//  }
//
//  /**
//   * Compare a word,frequency tuple with a collection of word, frequency tuples to find similar words by computing
//   * array's elements difference
//   * @param data collection of word, frequency to tuple to look into
//   * @param testedWord word that we want to find its similar word
//   * @param parameters L(0) contains the accepted difference between two array value that we accept
//   * @return words that are similar
//   */
//  def naiveDifference(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
//
//    val acceptedDifference = parameters(0)
//    // Test for left shift
//    for (i <- 0 to 200) {
//
//      var testedWordReduced = testedWord
//      testedWord._2.toList.drop(i)
//      //add the testedWord values to the arrays and compute difference for future comparison
//      val zipDataTestedWord = data.map(x => (x._1, testedWordReduced._2.zip(shiftList("left", i, x._2.toList)).map(x => math.abs(x._1 - x._2)), x._2))
//      //test similarity criteria between each data word array and the tested word
//      val booleanDataTestedWord = zipDataTestedWord.map(x => (x._1, x._2.map(y => y <= acceptedDifference)))
//      //filter the arrays that have at least one value that didn't pass the similarity test
//      booleanDataTestedWord.map(x => (x._1, x._2.filter(_ == false))).filter(x => x._2.length == 0 && x._1 != testedWord._1).map(x => (x._1))
//      //TODO return if we have a match during a loop
//    }
//    // Test for left shift
//    for (i <- 0 to 200) {
//
//      var testedWordReduced = testedWord
//      testedWord._2.toList.dropRight(i)
//      //add the testedWord values to the arrays and compute difference for future comparison
//      val zipDataTestedWord = data.map(x => (x._1, testedWordReduced._2.zip(shiftList("right", i, x._2.toList)).map(x => math.abs(x._1 - x._2)), x._2))
//      //test similarity criteria between each data word array and the tested word
//      val booleanDataTestedWord = zipDataTestedWord.map(x => (x._1, x._2.map(y => y <= acceptedDifference)))
//      //filter the arrays that have at least one value that didn't pass the similarity test
//      booleanDataTestedWord.map(x => (x._1, x._2.filter(_ == false))).filter(x => x._2.length == 0 && x._1 != testedWord._1).map(x => (x._1))
//      //TODO return if we have a match during a loop
//    }
//
//  }
//
//  /**
//   * Compare the ratio of word's frequency with the collection word's frequency to find similar words by computing
//   * the ratio line
//   * @param data collection of word, frequency to tuple to look into
//   * @param testedWord word that we want to find its similar word
//   * @param parameters L(0) contains the straightness of the curve that we accept
//   * @return words that are similar
//   */
//  def naiveDivision(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
//    val acceptedDifference = parameters(0)
//    //add the testedWord values to the arrays and compute division for future comparison
//    val dividedDataTestedWord = data.map(x => (x._1, testedWord._2.zip(x._2).map(x => x._1 / x._2), x._2))
//    //could be useful for testing purpose
//    //val tempDifferenceMaxMin = dividedDataTestedWord.map(x => (x._1, (((findMinAndMax(x._2))._2)- ((findMinAndMax(x
//    // ._2))._1))))
//    val booleanDataTestedWord = dividedDataTestedWord.map { x =>
//      val minMax = findMinAndMax(x._2)
//      (x._1, minMax._2 - minMax._1 < acceptedDifference)
//    }
//    booleanDataTestedWord.filter(x => x._2 && x._1 != testedWord._1).map(x => x._1)
//  }
//
//  /**
//   * Apply the max scaling function before calling the NaiveDifference similarity function
//   * @param data collection of word, frequency to tuple to look into
//   * @param testedWord word that we want to find its similar word
//   * @param parameters L(0) contains the accepted difference between two array value that we accept
//   * @return words that are similar
//   */
//  def naiveDifferenceScalingMax(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
//    naiveDifference(data.map(proportionalScalarMax(_)), proportionalScalarMax(testedWord), parameters)
//  }
//
//  /**
//   * Apply the average scaling function before calling the NaiveDifference similarity function
//   * @param data collection of word, frequency to tuple to look into
//   * @param testedWord word that we want to find its similar word
//   * @param parameters L(0) contains the accepted difference between two array value that we accept
//   * @return words that are similar
//   */
//  def naiveDifferenceScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
//    naiveDifference(data.map(proportionalScalarAverage(_)), proportionalScalarAverage(testedWord), parameters)
//  }
//
//  /**
//   * Apply the max scaling function before calling the NaiveDivision similarity function
//   * the ratio line
//   * @param data collection of word, frequency to tuple to look into
//   * @param testedWord word that we want to find its similar word
//   * @param parameters L(0) contains the straightness of the curve that we accept
//   * @return words that are similar
//   */
//  def naiveDivisionScalingMax(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
//    naiveDivision(data.map(proportionalScalarMax(_)), proportionalScalarMax(testedWord), parameters)
//  }
//
//  /**
//   * Apply the average scaling function before calling the NaiveDivision similarity function
//   * the ratio line
//   * @param data collection of word, frequency to tuple to look into
//   * @param testedWord word that we want to find its similar word
//   * @param parameters L(0) contains the straightness of the curve that we accept
//   * @return words that are similar
//   */
//  def naiveDivisionScalingAverage(data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double]): RDD[(String)] = {
//    naiveDivision(data.map(proportionalScalarAverage(_)), proportionalScalarAverage(testedWord), parameters)
//  }
//
//}