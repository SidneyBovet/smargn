import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import techniques.NaiveComparisons._
import org.apache.spark.rdd.RDD


class NaiveComparisonsTests extends SparkTestUtils with ShouldMatchers {

  test("testMetricDifference1") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(5.0, 3.0, 2.0, 5.0, 5.0))
    naiveDifferenceMetric(word1, word2, List(1, 0.2)) should be(7.0)
  }

  test("testMetricDifference2") {
    val word1 = ("blue", Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green", Array(0.0, 3.0, 2.0, 5.0, 5.0))
    naiveDifferenceMetric(word1, word2, List(1, 0.2)) should be(5.0)
  }

  test("testMetricDifference3") {
    val word1 = ("blue", Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green", Array(0.0, 3.0, 2.0, 10.0, 5.0))
    naiveDifferenceMetric(word1, word2, List(1, 0.2)) should be(Double.MaxValue)
  }

  test("testMetricDifference4") {
    val word1 = ("blue", Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green", Array(0.0, 3.0, 2.0, 10.0, 5.0))
    naiveDifferenceMetric(word1, word2, List(1, 2)) should be(10.0)
  }


  test("testMetricDivision1") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 4.0, 3.0, 5.0, 5.0))
    naiveDivisionMetric(word1, word2, List(1)) should be(0.5)
  }

  test("testMetricDivision2") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 3.0, 3.0, 5.0, 0.0))
    naiveDivisionMetric(word1, word2, List(1)) should be(Double.MaxValue)
  }


  sparkTest("testNaiveDifference") {
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)), ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = sc.parallelize(dataRaw)

    naiveDifference(data, testedWord, List(3.0, 0.0)).collect().sortWith(_ < _) should be(Array("orange", "flower").sortWith(_ < _))
  }

  sparkTest("testNaiveDifference2") {
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)), ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifference(data, testedWord, List(3.0, 0.2)).collect().sortWith(_ < _) should be(Array("blue", "yellow", "orange", "flower").sortWith(_ < _))
  }


  sparkTest("testNaiveDivision") {
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)), ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = sc.parallelize(dataRaw)
    naiveDivision(data, testedWord, List(0.8)).collect().sortWith(_ < _) should be(Array("flower").sortWith(_ < _))
  }


  test("testMetricDifferenceTopKMiddle") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(0.0, 3.0, 2.0, 5.0, 5.0))
    naiveDifferenceMetricTopK(word1, word2, List(1)) should be(0.8)
  }

  test("testMetricDifferenceTopKGood") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(0.0, 2.0, 3.0, 5.0, 5.0))
    naiveDifferenceMetricTopK(word1, word2, List(1)) should be(0.4)
  }

  test("testMetricDifferenceTopKPerfect") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    naiveDifferenceMetricTopK(word1, word2, List(1)) should be(0.0)
  }

  test("testMetricDifferenceTopKBad") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(5.0, 10.0, 2.0, 5.0, 7.0))
    naiveDifferenceMetricTopK(word1, word2, List(1)) should be(8.0)
  }

  test("testMetricDifferenceTopKWorst") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(5.0, 10.0, 20.0, 50.0, 55.0))
    naiveDifferenceMetricTopK(word1, word2, List(1)) should be(125.0)
  }

  test("testMetricDivisionTopKNormal") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 4.0, 3.0, 5.0, 5.0))
    naiveDivisionMetricTopK(word1, word2, List(1)) should be(0.5)
  }


  test("testMetricDivisionTopK0Num") {
    val word1 = ("blue", Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 4.0, 3.0, 5.0, 5.0))
    naiveDivisionMetricTopK(word1, word2) should be(2.5)
  }

  test("testMetricDivisionTopK0Den") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 4.0, 0.0, 5.0, 5.0))
    naiveDivisionMetricTopK(word1, word2) should be(2.5)
  }

  test("testMetricDivisionTopK0DenNum") {
    val word1 = ("blue", Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green", Array(1.0, 4.0, 0.0, 5.0, 5.0))
    naiveDivisionMetricTopK(word1, word2) should be(0.5)
  }

}
