import org.scalatest._
import techniques.DynamicTimeWrapping._

/**
 * Created by mathieu and ana on 09/04/15.
 */
class DTWTests extends SparkTestUtils with ShouldMatchers {
  test("DTW simple test") {
    val word1 = Array[Double](1, 1, 1, 9, 1)
    val word2 = Array[Double](1, 1, 10, 1, 1)
    dynamicTimeWrappingMetric(word1, word2, 2) should be(1)
  }

  test("DTW complex test") {
    val word1 = Array[Double](1, 1, 1, 9, 10, 12, 4, 1)
    val word2 = Array[Double](1, 1, 10, 11, 13, 5, 1, 1)
    dynamicTimeWrappingMetric(word1, word2, 2) should be(4)
  }

  sparkTest("DTW comparison test") {
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)),
      ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)),
      ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = sc.parallelize(dataRaw)

    dtwComparison(data, testedWord, List(7.0)).collect().sortWith(_ < _) should
      be(Array("orange").sortWith(_ < _))
  }
}
