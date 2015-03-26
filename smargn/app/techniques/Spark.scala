package techniques

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Valentin on 26/03/15.
 */
object Spark {
  val ctx = new SparkContext(new SparkConf().setAppName("naiveCompare").setMaster("local[2]"))
  //                          .setMaster("yarn-client")
}
