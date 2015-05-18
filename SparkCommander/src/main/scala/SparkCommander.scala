import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import techniques._
import utils.{Launcher, SubTechniques, HDFSHandler}
import utils.Launcher._
import utils.MD5.hash

/*
 * Contributors:
 *  - Valentin Rutz: createOutput, parser (not mode), main (not mode or compare)
 */

/*
 * Contributors:
 *  - Valentin Rutz: createOutput, parser (not mode), main (not mode or compare)
 */

/**
 * From Valentin with love on 21/04/15.
 */
object SparkCommander {

  val INPUT = "hdfs:///projects/temporal-profiles/data-generation/clean-1gram"
  val BASE_PROFILE = "hdfs:///projects/temporal-profiles/data-generation/baseProfile"

  private def createOutput(hash: String): String = {
    "hdfs:///projects/temporal-profiles/results/" + hash + "/"
  }


  /**
   * Arguments parsing representation class
   * @param words the words to search
   * @param technique the technique to use
   * @param parameters the parameter for that technique
   */
  private case class Config(mode: String = "", words: Seq[String] = Seq[String](), technique: String = "", hash: String = "",
                            range: Range = Launcher.startYear to Launcher.endYear, parameters: Seq[Double] = Seq[Double]())


  private val parser = new OptionParser[Config]("scopt") {
    head("SparkCommander", "1.0")

    opt[String]('m', "mode") optional() action { (mode, config) => config.copy(mode = mode)
    } text "optional mode of working, for comparing two words"
    opt[String]('h', "hash") valueName "<hash>" action { (hash, config) => config.copy(hash = hash)
    } text "The hash where we put the results"
    opt[Seq[String]]('w', "words") valueName "<word1>,<word2>,..." action { (words, config) => config.copy(words = words)
    } text "The words you want to search"
    opt[String]('t', "technique") action { (technique, config) => config.copy(technique = technique)
    } text "The technique you want to use"
    opt[Seq[Int]]('r', "range") valueName "startOfRange,endOfRange" action {
      case (Seq(start, end), config) => config.copy(range = start to end)
    }
    opt[Seq[Double]]('p', "parameters") valueName "<param1>,<param2>..." optional() action {
      (parameters, config) => config.copy(parameters = parameters)
    } text "Optional parameters for this technique"
  }

  /**
   *
   * @param args must be in the format: [-m mode]? -w word1,word2?,...  -t technique_name [-r startOfRange,endOfRange]? [-p param1,param2,...]?
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SparkCommander").setMaster("yarn-cluster").set("num-executors", "25")

    @transient val sc = new SparkContext(conf)

    parser.parse(args, Config(mode = null, words = Seq(), technique = null, hash = null,
      range = Launcher.startYear to Launcher.endYear, parameters = Seq())) match {
      case Some(Config(mode, words, technique, hash, range, parameters)) =>
        val output = createOutput(hash)

        Logger.getLogger("SparkCommander").debug("output folder is " + output)
        val hdfs = new HDFSHandler(sc.hadoopConfiguration)
        // Create folder for results
        hdfs.createFolder(output)

        val tech: Technique = technique match {
          // Add your technique methods here. All lowercase for the name pliz
          case "naivedifference" => NaiveComparisons.naiveDifferenceTopKScalingAverage
          case "naivedivision" => NaiveComparisons.naiveDivisionTopKScalingAverage
          case "naivedifferencesquared" => NaiveComparisons.naiveDifferenceSquaredTopKScalingAverage
          case "naivedivisionvar" => NaiveComparisons.naiveDivisionVarianceTopKScalingAverage
          case "inverse" => NaiveComparisons.naiveInverseDifference
          case "shift" => NaiveComparisons.naiveDifferenceScalingAverageWithShifting
          case "divergence" => Divergence.naiveDifferenceDivergence
          case "smarterdivergence" => SubTechniques.smarterDivergence
          case "peaks" => PeakComparison.peakComparisonWithMeanDerivative
          case "dtw" => DynamicTimeWrapping.dtwComparison
          case "dtwtopk" => DynamicTimeWrapping.dtwSimpleTopK
          case "dtwscaleavgtopk" => DynamicTimeWrapping.dtwComparisonScaleAvgTopK
          case "dtwscalemaxtopk" => DynamicTimeWrapping.dtwComparisonScaleMaxTopK
          case "peakstopk" => PeakComparison.peaksTopK
        }


        mode match {
          case "compare" => runCompare(words, INPUT, BASE_PROFILE, output, parameters.toList, tech, sc)
          case "findparams" => runParamsFinding(sc, INPUT, BASE_PROFILE)
          case "preprocessing" => runPreprocessing(sc, INPUT, BASE_PROFILE, output)
          case _ => runList(words, INPUT, BASE_PROFILE, output, parameters.toList, tech, sc)
        }
        hdfs.close()
      //runCompare(words, INPUT, BASE_PROFILE, output, parameters.toList, tech, sc)

      case None => // Bad arguments
    }

    sc.stop()
  }
}
