import org.scalatest.ShouldMatchers


/**
 * Created by mathieu on 25/03/15.
 */
class ExampleTest extends SparkTestUtils with ShouldMatchers {

  /**
   * Spark test example. You should use sparkTest when testing Spark code, it gives access to the SparkContext (sc).
   * If you want to test only Spark code, use the following command in sbt: test-only -- -n com.bd.test.tags.SparkTest,
   * and to run only non-Spark tests: test-only -- -l com.bd.test.tags.SparkTest
   */
  sparkTest("spark filter test") {
    val data = sc.parallelize(1 to 1e6.toInt)
    data.filter {
      _ % 2 == 0
    }.count should be(5e5.toInt)
  }

  test("normal addition test") {
    val x = 17
    val y = 9
    x + y should be(26)
  }
}
