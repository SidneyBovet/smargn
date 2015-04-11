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

  test("mean test") {
    val a = Array(0, 1, 2, 3, 4)
    utils.Filtering.mean(a) should be(2)
  }
}
