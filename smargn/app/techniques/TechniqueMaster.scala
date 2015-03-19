package techniques

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Valentin on 18/03/15.
 */
class TechniqueMaster {
  val conf = new SparkConf().setAppName("naiveCompare").setMaster("local[2]")
  //                          .setMaster("yarn-client")
  val spark = new SparkContext(conf)
}
