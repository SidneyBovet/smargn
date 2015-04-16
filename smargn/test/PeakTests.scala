import org.scalatest._
import techniques.PeakComparison._

class PeakTests extends SparkTestUtils with ShouldMatchers {
  test("testMetricDerivative1") {
    val word1 = Array[Double](1, 1, 1, 199, 1, 1, 1)
    peakDerivativeMetric(("whatever", word1), ("plop", word1), 1) should be(1.0)
  }

  test("windowPeakDerivativeDetection1") {
    val word1 = Array[Double](1, 1, 1, 199, 1, 1, 1)
    windowPeakDerivative(("a", word1), 1, 1) should be(List((4, 198, -198)))
  }

  test("testMetricDerivative2") {
    val word1 = Array[Double](3, 1, 1, 1, 199, 1, 1, 1, 200, 1, 6)
    peakDerivativeMetric(("whatever", word1), ("plop", word1), 1) should be(1.0)
  }

  test("windowPeakDerivativeDetection2") {
    val word1 = Array[Double](3, 1, 1, 1, 199, 1, 1, 1, 200, 1, 6)
    windowPeakDerivative(("a", word1), 1, 1) should be(List((5, 198, -198), (9, 199, -199)))
  }

  test("testMetricMinMax") {
    val word1 = Array[Double](2, 2, 1, 199, 1, 3, 2, 3, 2, 2, 2, 2, 3, 2, 3, 2, 3, 2, 2, 3, 2, 3, 2, 3, 2, 3, 2)
    peakMaxMinMetric(("whatever", word1), ("plop", word1), 6, 3, 5) should be(1.0)
  }

  test("windowPeakMinMaxDetection") {
    val word1 = Array[Double](2, 2, 1, 199, 1, 3, 2, 3, 2, 2, 2, 2, 3, 2, 3, 2, 3, 2, 2, 3, 2, 3, 2, 3, 2, 3, 2)
    filterDuplicateYears(windowPeakMinMax(("a", word1), 5, 2)) should be(List((3, 198, 198)))
  }

  test("testMetricMean") {
    val word1 = Array[Double](1, 1, 1, 199, 1, 3, 2, 3, 2, 2, 2, 2, 3, 2, 3, 2, 3, 2, 2, 3, 2, 3, 2, 3, 2, 3, 2)
    peakMeanMetric(("whatever", word1), ("plop", word1), 4, 3, 0) should be(1.0)
  }

  test("windowPeakMeanDetection") {
    val word1 = ("whatever", Array[Double](2, 4, 5, 199, 3, 2, 1))
    windowPeakMean(word1 , 1, 0) should be(List((3,197,198)))
  }

  test("windowPeakMeanDetection2") {
    val word2 = ("bis",Array(2.0, 4.0, 5.0, 199.0, 1.0, 10.0, 2.0, 4.0, 30.0, 300.0, 2.0, 1.0))
    windowPeakMean(word2 , 1, 0) should be(List((3,197.0,198.0), (5,9.0,8.0), (9,298.0,299.0)))
  }

  test("windowPeakMeanDetection3") {
    val word2 = ("bis",Array(2.0, 4.0, 5.0, 199.0, 1.0, 10.0, 2.0, 4.0, 30.0, 300.0, 2.0, 1.0))
    windowPeakMean(word2 , 10, 0) should be(List((9,299.0,299.0)))
  }

}
