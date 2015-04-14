import org.scalatest._

/**
 * Created with love by sidney.
 */
class FilteringTest extends SparkTestUtils with ShouldMatchers {

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
  test("flat profile test") {
    val data: Array[Double] = Array(1, 1, 1, 1)
    utils.Filtering.isNotFlat(data, 0.1) should be(false)
    utils.Filtering.countNotFlat(data, 0.1) should be(0)
  }
  test("nonflat profile test") {
    val data: Array[Double] = Array(1, 1, 1, 10)
    utils.Filtering.isNotFlat(data, 0.1) should be(true)
    utils.Filtering.countNotFlat(data, 0.1) should be(1)
  }
}
