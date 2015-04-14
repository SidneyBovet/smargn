import org.scalatest._

/**
 * Created with love by sidney.
 */
class FilteringTest extends SparkTestUtils with ShouldMatchers {

  /* sample spark test to be used for a broader testing of the filtering
  sparkTest("spark filter test") {
    val data = sc.parallelize(1 to 1e6.toInt)
    data.filter {
      _ % 2 == 0
    }.count should be(5e5.toInt)
  }
  */
  val sampleData =
    """
      |turing 1 2 4 8 16 32
      |lovelace 100 10 100 10 100 10
      |dijsktra 101 95 109 98 110 103
      |lamport 100 90 80 70 60 50
      |wozniak 65 56 65 56 65 65
      |brin 29 32 33 32 34 36
      |page 43 42 41 40 41 42
      |jobs 23 34 45 56 67 178
      |gates 12 18 39 38 17 10
    """.stripMargin

  /* Mean testing */
  test("std mean test") {
    val a: Array[Double] = Array(0, 1, 2, 3, 4)
    utils.Filtering.mean(a) should be(2)
  }

  test("single mean test") {
    val a = Array(42.42)
    utils.Filtering.mean(a) should be(42.42)
  }

  test("zero mean test") {
    val a: Array[Double] = Array()
    utils.Filtering.mean(a) should be(0)
  }

  /* Peak detection */
}
