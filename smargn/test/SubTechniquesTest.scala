import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.ShouldMatchers
import techniques.NaiveShiftComparison._
import techniques.NaiveInverseComparisons._

class SubTechniquesTest extends SparkTestUtils with ShouldMatchers {
  val conf = new SparkConf().setAppName("naiveCompare").setMaster("local[4]").set("spark.driver.allowMultipleContexts", "true")
  val spark = new SparkContext(conf)

  test("testNaiveInverseComparisons1") {
    val testedWord = ("sky", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("sea", Array(3.0, 11.0, 3.0, 2.0, 1.0)), ("earth", Array(3.0, 12.0, 4.0, 3.0, 2.0)), ("yellow", Array(13.0, 14.0, 13.0, 16.0, 15.0)), ("flower", Array(14.0, 15.0, 16.0, 114.0, 15.0)))
    val data = spark.parallelize(dataRaw)
    val b = naiveInverseDifference(data, testedWord, List(2.0))
    b.collect().sortWith(_ < _) should be(Array("earth", "sea"))
  }

  test("testNaiveInverseComparisons2") {
    val testedWord = ("sky", Array(12.0, 13.0, 14.0, 112.0, 13.0))
    val dataRaw = Array(("sea", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("earth", Array(2.0, 1.0, 2.0, 4.0, 4.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)), ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = spark.parallelize(dataRaw)
    naiveInverseDifference(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  // SHIFT TEST BASED ON VALUE SHIFTLEN = 3, SHIFTSTEP = 1
  test("NaiveShiftComparison1") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0, 3.0)), ("boring", Array(15.0, 16.0, 71.0, 8.0, 19.0, 10.0, 11.0, 12.0, 13.0)))
    val data = spark.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  test("NaiveShiftComparison2") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(10.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0)), ("boring", Array(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0)))
    val data = spark.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  test("NaiveShiftComparison3") {

    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 11.0, 12.0, 13.0)), ("boring", Array(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0)))
    val data = spark.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2, 3)).collect().sortWith(_ < _) should be(Array())
  }
}