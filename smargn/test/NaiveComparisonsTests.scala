import org.scalatest._
import techniques.NaiveComparisons._
import org.apache.spark.rdd.RDD
import techniques.Spark


class NaiveComparisonsTests extends SparkTestUtils with ShouldMatchers {
  test("testMetricDifference1") {
    val word1 = ("blue",Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green",Array(5.0, 3.0, 2.0, 5.0, 5.0))
    naiveDifferenceMetric(word1,word2, 1, 1) should be(7.0)
  }

  test("testMetricDifference2") {
    val word1 = ("blue",Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green",Array(0.0, 3.0, 2.0, 5.0, 5.0))
    naiveDifferenceMetric(word1,word2, 1, 1) should be(5.0)
  }

  test("testMetricDifference3") {
    val word1 = ("blue",Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green",Array(0.0, 3.0, 2.0, 10.0, 5.0))
    naiveDifferenceMetric(word1,word2, 1, 1) should be(Double.MaxValue)
  }

  test("testMetricDifference4") {
    val word1 = ("blue",Array(1.0, 2.0, 0.0, 4.0, 5.0))
    val word2 = ("green",Array(0.0, 3.0, 2.0, 10.0, 5.0))
    naiveDifferenceMetric(word1,word2, 1, 2) should be(10.0)
  }


  test("testMetricDivision1") {
    val word1 = ("blue",Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green",Array(1.0, 4.0, 3.0, 5.0, 5.0))
    naiveDivisionMetric(word1,word2, 1) should be(0.5)
  }

  test("testMetricDivision2") {
    val word1 = ("blue",Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("green",Array(1.0, 3.0, 3.0, 5.0, 0.0))
    naiveDivisionMetric(word1,word2, 1) should be(Double.MaxValue)
  }

/*
  test("testNaiveDifference") {
    val spark = Spark.ctx
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("blue",Array(1.0, 2.0, 3.0, 4.0, 5.0)),("yellow",Array(3.0, 4.0, 3.0, 6.0, 5.0)),("flower",Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange",Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy",Array(20.0, 30.0, 4.0, 2.0, 3.0)),("parrot",Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = spark.parallelize(dataRaw)
    naiveDifference(data, testedWord, List(3.0,0.0)) should be("orange", "flower")
  }

  test("testNaiveDifference2") {
    val spark = Spark.ctx
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("blue",Array(1.0, 2.0, 3.0, 4.0, 5.0)),("yellow",Array(3.0, 4.0, 3.0, 6.0, 5.0)),("flower",Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange",Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy",Array(20.0, 30.0, 4.0, 2.0, 3.0)),("parrot",Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = spark.parallelize(dataRaw)
    naiveDifference(data, testedWord, List(3.0,1.0)) should be("blue", "yellow", "orange", "flower")
  }*/



}
