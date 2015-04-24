import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

/**
 * Created by mathieu on 27/03/15.
 * Code adapted from http://blog.quantifind.com/posts/spark-unit-test/
 */
object SparkTest extends org.scalatest.Tag("com.bd.test.tags.SparkTest")

trait SparkTestUtils extends FunSuite {

  import utils.Formatting._

  var sc: SparkContext = _

  /**
   * Convenience method for tests that use spark. Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off.
   *
   * By default, it turns off spark logging, because it just clutters up the test output. However,
   * when you are actively debugging one test, you may want to turn the logs on.
   *
   * @param name the name of the test
   * @param silenceSpark true to turn off spark logging
   */
  def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
    test(name, SparkTest) {
      val origLogLevels: Map[String, Level] = if (silenceSpark) {
        SparkUtil.silenceSpark()
      } else {
        Map[String, Level]()
      }
      sc = new SparkContext("local", name)
      try {
        body
      } finally {
        sc.stop()
        sc = null
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
        if (silenceSpark) {
          SparkUtil.setOldLogLevels(origLogLevels)
        }
      }
    }
  }
}

object SparkUtil {
  def silenceSpark(): Map[String, Level] = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]): Map[String, Level] = {
    loggers.map { loggerName =>
      val logger = Logger.getLogger(loggerName)
      val prevLevel = logger.getLevel
      logger.setLevel(level)
      loggerName -> prevLevel
    }.toMap
  }

  def setOldLogLevels(logsLevel: Map[String, Level]): Unit = {
    logsLevel.foreach { case (loggerName, loggerLevel) =>
      val logger = Logger.getLogger(loggerName)
      logger.setLevel(loggerLevel)
    }
  }
}