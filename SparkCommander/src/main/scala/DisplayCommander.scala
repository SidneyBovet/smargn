import masters.Displayer._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import utils.HDFSHandler

/**
 * From Valentin with love on 03/05/15.
 */
object DisplayCommander {
  private val INPUT = "hdfs:///projects/temporal-profiles/data-generation/clean-1gram"

  private def createOutput(words: Seq[String]): String =
    "hdfs:///projects/temporal-profiles/results/" + words.mkString("-") + "/"

  /**
   * Arguments parsing representation class
   * @param words the words to search
   */
  case class Config(words: Seq[String] = Seq[String]())

  private val parser = new OptionParser[Config]("scopt") {
    head("DisplayerCommander", "1.0")

    opt[Seq[String]]('w', "words") valueName "<word1>,<word2>,..." action {
      (words, config) => config.copy(words = words)
    } text "The words you want to search"
  }

  /**
   *
   * @param args must be in the format: -w word1,word2?,...  -t technique_name -p param1?,param2?,...
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SparkCommander")
      .setMaster("yarn-cluster")
      .set("num-executors", "25")

    val sc = new SparkContext(conf)

    parser.parse(args, Config(words = Seq())) match {
      case Some(Config(words, technique, parameters)) =>
        val output = createOutput(words)

        val hdfs = new HDFSHandler(sc.hadoopConfiguration)
        // Create folder for results
        hdfs.createFolder(output)
        hdfs.close()

        runList(words, INPUT, output, sc)
      case None => // Bad arguments
    }

    sc.stop()
  }
}
