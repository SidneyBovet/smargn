import org.scalatest._

/**
 * Created by joanna on 26.04.15.
 */
class TopKTest extends SparkTestUtils with ShouldMatchers {

  sparkTest("testTopK1") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("yellow", Array(0.0, 4.0, 3.0, 6.0, 5.0))
    val word3 = ("flower", Array(6.0, 5.0, 6.0, 14.0, 5.0))
    val word4 = ("orange", Array(2.0, 5.0, 6.0, 13.0, 2.0))
    val word5 = ("rainbow", Array(3.0, 5.0, 6.0, 13.0, 2.0))
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))

    val preData = Array[(String, Array[Double])](word1, word2, word3, word4, word5)

    val data = sc.parallelize(preData)


    val metric = (x: (String, Array[Double]), y: (String, Array[Double]), parameters: List[Double]) => math.abs(x._2(0) - y._2(0))
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1

    TopK.retrieveTopK(3, metric, data, testedWord, order) should be(List("orange", "blue", "rainbow"))

  }


  sparkTest("testTopK2") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("yellow", Array(0.0, 4.0, 3.0, 6.0, 5.0))
    val word3 = ("flower", Array(6.0, 5.0, 6.0, 14.0, 5.0))
    val word4 = ("orange", Array(2.0, 5.0, 6.0, 13.0, 2.0))
    val word5 = ("rainbow", Array(10.0, 5.0, 6.0, 13.0, 2.0))
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))

    val preData = Array[(String, Array[Double])](word1, word2, word3, word4, word5)

    val data = sc.parallelize(preData)


    val metric = (x: (String, Array[Double]), y: (String, Array[Double]), parameters: List[Double]) => math.abs(x._2(0) - y._2(0))
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1

    TopK.retrieveTopK(4, metric, data, testedWord, order) should be(List("orange", "blue", "yellow", "flower")
    )

  }


  sparkTest("testTopK3") {
    val word1 = ("blue", Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val word2 = ("yellow", Array(0.0, 4.0, 3.0, 6.0, 5.0))
    val word3 = ("flower", Array(6.0, 5.0, 6.0, 14.0, 5.0))
    val word4 = ("orange", Array(2.0, 5.0, 6.0, 13.0, 2.0))
    val word5 = ("rainbow", Array(10.0, 5.0, 6.0, 13.0, 2.0))
    val testedWord = ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0))

    val preData = Array[(String, Array[Double])](word1, word2, word3, word4, word5)

    val data = sc.parallelize(preData)


    val metric = (x: (String, Array[Double]), y: (String, Array[Double]), parameters: List[Double]) => math.abs(x._2(0) - y._2(0))
    val order = (x: (String, Double), y: (String, Double)) => if (x._2 != y._2) {
      x._2 < y._2
    } else x._1 < y._1

    TopK.retrieveTopK(10, metric, data, testedWord, order) should be(List("orange", "blue", "yellow", "flower", "rainbow")
    )
  }

}
