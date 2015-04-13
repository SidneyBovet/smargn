import org.scalatest._
import techniques.DynamicTimeWrapping._

/**
 * Created by mathieu and ana on 09/04/15.
 */
class DTWTests extends SparkTestUtils with ShouldMatchers {
  test("DTW simple test") {
    val word1 = Array[Double](1, 1, 1, 9, 1)
    val word2 = Array[Double](1, 1, 10, 1, 1)
    compare(word1, word2, 2) should be(1)
  }

  test("DTW complex test") {
    val word1 = Array[Double](1, 1, 1, 9, 10, 12, 4, 1)
    val word2 = Array[Double](1, 1, 10, 11, 13, 5, 1, 1)
    compare(word1, word2, 2) should be(4)
  }
}
