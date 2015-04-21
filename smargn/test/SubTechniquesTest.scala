import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.ShouldMatchers
import techniques.NaiveShiftComparison._
import techniques.NaiveInverseComparisons._
import techniques.Divergence._

class SubTechniquesTest extends SparkTestUtils with ShouldMatchers {

  sparkTest("testNaiveInverseComparisons1") {
    val testedWord = ("sky", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("sea", Array(3.0, 11.0, 3.0, 2.0, 1.0)), ("earth", Array(3.0, 12.0, 4.0, 3.0, 2.0)), ("yellow", Array(13.0, 14.0, 13.0, 16.0, 15.0)), ("flower", Array(14.0, 15.0, 16.0, 114.0, 15.0)))
    val data = sc.parallelize(dataRaw)
    val b = naiveInverseDifference(data, testedWord, List(2.0))
    b.collect().sortWith(_ < _) should be(Array("earth", "sea"))
  }

  sparkTest("testNaiveInverseComparisons2") {
    val testedWord = ("sky", Array(12.0, 13.0, 14.0, 112.0, 13.0))
    val dataRaw = Array(("sea", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("earth", Array(2.0, 1.0, 2.0, 4.0, 4.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)), ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = sc.parallelize(dataRaw)
    naiveInverseDifference(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  // SHIFT TEST BASED ON VALUE SHIFTLEN = 3, SHIFTSTEP = 1
  sparkTest("NaiveShiftComparison1") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0, 3.0)), ("boring", Array(15.0, 16.0, 71.0, 8.0, 19.0, 10.0, 11.0, 12.0, 13.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  sparkTest("NaiveShiftComparison2") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(10.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0)), ("boring", Array(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("NaiveShiftComparison3") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 11.0, 12.0, 13.0)), ("boring", Array(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("testDivergence1") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 10.0, 0.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  sparkTest("testDivergence2") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(2.0, 4.0, 2.0, 3.0, 2.0, 2.0, 5.0, 2.0, 2.0, 10.0, 0.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  // Here we would expect an empty array because we have two value in sea that differ form the word1. But the two curves
  // do not diverge
  sparkTest("testDivergence3") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 5.0, 2.0, 2.0, 19.0, 23.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("testDivergence4") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 10.0, 0.0)), ("sky", Array(13.0, 23.0, 33.0, 33.0, 0.0, 30.0, 0.0, 2.0, 2.0, 1.0, 0.0)), ("cloud", Array(3.0, 3.0, 3.0, 3.0, 3.0, 0.0, 0.0, 2.0, 2.0, 10.0, 0.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("cloud", "sea"))
  }

}