import org.apache.spark.rdd.RDD
import org.scalatest._
import techniques.PeakComparison._
import utils.Formatting._

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
    windowPeakMean(word1, 1, 0) should be(List((3, 197, 198)))
  }

  test("windowPeakMeanDetection1") {
    val word1 = ("whatever", Array[Double](2, 4, 5, 199, 1, 3, 2, 3, 2, 2, 2, 2, 3, 2, 3, 2, 3, 2, 2, 3, 2, 3, 2, 3, 2,
      3, 2))
    windowPeakMean(word1, 1, 10) should be(List((3, 197, 198)))
  }

  test("windowPeakMeanDetection2") {
    val word2 = ("bis", Array(2.0, 4.0, 5.0, 199.0, 1.0, 10.0, 2.0, 4.0, 30.0, 300.0, 2.0, 1.0))
    windowPeakMean(word2, 1, 0) should be(List((3, 197.0, 198.0), (5, 9.0, 8.0), (9, 298.0, 299.0)))
  }

  test("windowPeakMeanDetection3") {
    val word2 = ("bis", Array(2.0, 4.0, 5.0, 199.0, 1.0, 10.0, 2.0, 4.0, 30.0, 300.0, 2.0, 1.0))
    windowPeakMean(word2, 10, 0) should be(List((9, 299.0, 299.0)))
  }

  test("windowPeakMeanDerivativeDetection") {
    val word2 = ("bis", Array(1, 1, 1, 1, 1, 2.2, 2.3, 2.4, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 16.1, 16.2,
      20, 21, 22, 23, 24, 2.0, 4.0, 5.0, 199.0, 1.0, 10.0, 2.0, 4.0, 30.0, 300.0, 2.0, 1.0))
    windowPeakMeanDerivative(word2, 2, 1) should
      be(List((32, 65.66666666666667, 198.0), (34, 9.0, 8.0), (38, 99.33333333333333, 149.5)))
  }

  /*sparkTest("Coupabe - Crime ") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("crime", "coupable")).collect()
    println(peakMeanMetric(words(0), words(1)))
  }

  sparkTest("Coupabe - Crime 2") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("crime", "coupable")).collect()
    println(peakMaxMinMetric(words(0), words(1), 10, 10))
  }
  sparkTest("Coupabe - Crime (derivative)") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("crime", "coupable")).collect()
    val met = peakDerivativeMetric(words(0), words(1))
    println(met)
    met should be > 0.5
  }
  sparkTest("Coupabe - Droite (derivative)") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("crime", "droite")).collect()
    val met = peakDerivativeMetric(words(0), words(1))
    println(met)
    met should be < 0.5
  }

  sparkTest("Gauche - Droite (derivative)") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("gauche", "droite")).collect()
    val met = peakDerivativeMetric(words(0), words(1), 5)
    println(met)
    met should be > 0.5
  }
  sparkTest("Landsgemeinde - Appenzell (derivative)") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("landsgemeinde", "appenzell")).collect()
    val met = peakDerivativeMetric(words(0), words(1), 3)
    println(met)
    met should be > 0.5
  }
  sparkTest("Landsgemeinde - Crime (derivative)") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("landsgemeinde", "crime")).collect()
    val met = peakDerivativeMetric(words(0), words(1), 3)
    println(met)
    met should be < 0.5
  }

  sparkTest("Rire - Sourire (derivative)") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("rire", "sourire")).collect()
    val met = peakDerivativeMetric(words(0), words(1), 3)
    println(met)
    met should be > 0.5
  }

  sparkTest("Similar words to rire") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("rire", "sourire", "crime", "vin", "EPFL", "avocat"))
    val testedWord = words.filter(w => w._1 == "rire").collect().head
    val derivativeWords = peakComparisonWithDerivative(words, testedWord, List(0.5, 10, 1, 1))
    derivativeWords.collect should be(Array("rire", "sourire"))

  }

  sparkTest("Similar words to crime") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("rire", "sourire", "crime", "vin", "EPFL", "avocat"))
    val testedWord = words.filter(w => w._1 == "crime").collect().head
    val derivativeWords = peakComparisonWithDerivative(words, testedWord, List(0.5, 10, 2, 1))
    derivativeWords.collect should be(Array("crime", "coupable"))

  }

  sparkTest("Similar words rire from data") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    val words = searchWordFormatter(data, List("rire"))
    val testedWord = words.filter(w => w._1 == "rire").collect().head
    val derivativeWords = peakComparisonWithMeanDerivative(data, testedWord, List(0.5, 5, 2, 1))
    derivativeWords.collect should be(Array("rire", "sourire"))
  }

   sparkTest("Rire - Sourire (derivative-mean)") {
     val inputDir = "input"
     val data = dataFormatter(sc.textFile(inputDir)) //parse data
     val words = searchWordFormatter(data, List("rire", "crime")).collect()
     val met = peakMeanDerivativeMetric(words(0), words(1), 5, 1, 1)
     println(met)
     met should be < 0.5
   }*/

  sparkTest("count") {
    val inputDir = "input"
    val data = dataFormatter(sc.textFile(inputDir)) //parse data
    println(data.collect().size)
  }
}
