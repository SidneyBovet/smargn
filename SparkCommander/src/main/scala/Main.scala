import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import techniques.{NaiveShiftComparison, NaiveInverseComparisons, NaiveComparisons}
import utils.Launcher._

/**
 * Created by Valentin on 21/04/15.
 */
object Main {

  private val INPUT = "hdfs:///projects/temporal-profiles/data-generation/clean-1gram"

  private def createOutput(words: Seq[String], technique: String, params: Seq[Double]): String =
    s"hdfs://projects/temporal-profiles/results/${words.mkString("-")}${
      if (params.nonEmpty) s"_${technique.toLowerCase}_${
        params.mkString("-")
      }"
      else ""
    }/"

  case class Config(words: Seq[String], technique: String, parameters: Seq[Double])

  private val parser = new OptionParser[Config]("scopt") {
    head("SparkCommander", "1.0")
    opt[Seq[String]]('w', "words") action {
      (words, config) => config.copy(words = words)
    } text "The words you want to search"
    opt[String]('t', "technique") action {
      (technique, config) => config.copy(technique = technique)
    } text "The technique you want to use"
    opt[Seq[Double]]('p', "parameters") action {
      (parameters, config) => config.copy(parameters = parameters)
    } text "The parameters for this technique"
  }

  /**
   *
   * @param args must be in the format:
   *             -w word1, word2?, ...  -t technique_name -p param1?, param2?, ...
   *
   */
  def main(args: List[String]) = {
    val conf = new SparkConf().setAppName("SparkCommander")
    //                 .setMaster("yarn-cluster")

    val sc = new SparkContext(conf)

    parser.parse(args, Config) match {
      case Some(Config(words, technique, parameters)) =>
        val output = createOutput(words, technique, parameters)
        val tech: (RDD[(String, Array[Double])], (String, Array[Double]), List[Double]) => RDD[(String)] = technique match {
          case "Naive" => NaiveComparisons.naiveDifferenceScalingMax
          case "Inverse" => NaiveInverseComparisons.naiveInverseDifference
          case "Shift" => NaiveShiftComparison.naiveDifferenceShift
          case _ => NaiveComparisons.naiveDifferenceScalingMax
        }
        runList(words.toList, INPUT, output, parameters.toList, tech, sc)
      case None => // Bad arguments
    }

    sc.stop()
  }
}
