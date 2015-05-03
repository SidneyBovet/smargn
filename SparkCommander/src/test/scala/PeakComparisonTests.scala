
import org.apache.spark.SparkContext
import org.scalatest._
import techniques.PeakComparison._
import utils.Formatting._

/**
 * Created by zhivka on 03.05.15.
 */
class PeakComparisonTests extends FunSuite with ShouldMatchers {
  /*test("guerre") {
    val sc = new SparkContext("local", "whatever")
    val inputDir = "input"
    val data = dataFormatter2(sc.textFile(inputDir))
    val words = searchWordFormatter(data,
      List("guerre", "armée"))
    val testedWord = words.filter(_._1 == "guerre").collect.head
    val arme = words.filter(_._1 == "armée").collect.head
    println(testedWord._1 + " ")
    println(arme._1 + " ")


    val params1: List[Double] = List(0.5, 3, 15, 2)

    val metric1 = peakComparisonWithMeanDerivative(data, testedWord, params1)

    println("Peak based on Derivative " + metric1.collect.mkString(" "))

    println("Peaks are ")
    words.foreach { x =>
      println(x._1 + " " + peakMeanDerivative(x, 5, 15, 2))
    }


  }*/
  //  test("Peak comparisons dummy") {
  //    val data = Array[Double](2, 5, 3, 1, 2, 1, 3, 5, 7, 12, 7, 5, 3, 1, 2, 1, 2)
  //    peakMeanDerivative(("hello", data), 3, 5, 2) should be((9, 5, 12) :: Nil)
  //  }

}
