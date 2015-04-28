import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import techniques.{NaiveComparisons, NaiveInverseComparisons, NaiveShiftComparison}
import utils.HDFSHandler
import utils.Launcher._

/**
 * From Valentin with love on 21/04/15.
 */
object SparkCommander {

  private val INPUT = "hdfs:///projects/temporal-profiles/data-generation/clean-1gram"

  private def createOutput(words: Seq[String], technique: String, params: Seq[Double]): String =
    s"hdfs:///projects/temporal-profiles/results/${words.mkString("-")}${
      if (params.nonEmpty) s"_${technique.toLowerCase}_${
        params.mkString("-")
      }"
      else ""
    }/"

  /**
   * Arguments parsing representation class
   * @param words the words to search
   * @param technique the technique to use
   * @param parameters the parameter for that technique
   */
  case class Config(words: Seq[String] = Seq[String](), technique: String = "", parameters: Seq[Double] = Seq[Double]())

  private val parser = new OptionParser[Config]("scopt") {
    head("SparkCommander", "1.0")

    opt[Seq[String]]('w', "words") valueName "<word1>,<word2>,..." action {
      (words, config) => config.copy(words = words)
    } text "The words you want to search"
    opt[String]('t', "technique") action {
      (technique, config) => config.copy(technique = technique)
    } text "The technique you want to use"
    opt[Seq[Double]]('p', "parameters") valueName "<param1>,<param2>..." optional() action {
      (parameters, config) => config.copy(parameters = parameters)
    } text "Optional parameters for this technique"
  }

  /**
   *
   * @param args must be in the format: -w word1,word2?,...  -t technique_name -p param1?,param2?,...
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SparkCommander")
      .setMaster("yarn-cluster")
      .set("num-executors", "25")

    @transient val sc = new SparkContext(conf)

    parser.parse(args, Config(words = Seq(), technique = null, parameters = Seq())) match {
      case Some(Config(words, technique, parameters)) =>
        val output = createOutput(words, technique, parameters)

        val hdfs = new HDFSHandler(sc.hadoopConfiguration)
        // Create folder for results
        hdfs.createFolder(output)
        hdfs.close()

        val tech: Technique = technique match {
          case "Naive" => NaiveComparisons.naiveDifferenceScalingMax
          case "Inverse" => NaiveInverseComparisons.naiveInverseDifference
          case "Shift" => NaiveShiftComparison.naiveDifferenceShift
          case _ => NaiveComparisons.naiveDifferenceScalingMax
        }

        runList(words, INPUT, output, parameters.toList, tech, sc)
      case None => // Bad arguments
    }

    sc.stop()
  }
}
