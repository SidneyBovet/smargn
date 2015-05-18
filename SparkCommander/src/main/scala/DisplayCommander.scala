import masters.Displayer._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import SparkCommander._
import utils.MD5.hash

/*
 * Contributors:
 *  - Valentin Rutz
 */
/**
 * From Valentin with love on 03/05/15.
 */
object DisplayCommander {
  private def createOutput(hash: String): String =
    "hdfs:///projects/temporal-profiles/results/" + hash + "/"

  /**
   * Arguments parsing representation class
   * @param words the words to search
   */
  private case class Config(words: Seq[String] = Seq[String](), hash: String = "")

  private val parser = new OptionParser[Config]("scopt") {
    head("DisplayerCommander", "1.0")

    opt[String]('h', "hash") valueName "<hash>" action { (hash, config) => config.copy(hash = hash)
    }
    opt[Seq[String]]('w', "words") valueName "<word1>,<word2>,..." action { (words, config) => config.copy(words = words)
    } text "The words you want to search" text "The hash where we put the results"
  }

  /**
   *
   * @param args must be in the format: -w word1,word2?,...
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("DisplayCommander").setMaster("yarn-cluster").set("num-executors", "25")

    val sc = new SparkContext(conf)

    parser.parse(args, Config(words = Seq(), hash = "")) match {
      case Some(Config(words, hash)) =>
        runList(words, BASE_PROFILE, INPUT, createOutput(hash), sc)
      case None => // Bad arguments
    }

    sc.stop()
  }
}
