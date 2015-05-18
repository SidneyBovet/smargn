package controllers

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Valentin on 26/03/15.
 */
object Spark {
  private var sc: Option[SparkContext] = None

  def ctx: SparkContext = {
    if (sc.isEmpty) {
      sc = Some(new SparkContext(new SparkConf().setAppName("naiveCompare").setMaster("local[2]")))
      //                          .setMaster("yarn-client")
      //                          .setMaster("spark://icdataprotal2:.../...")))
    }
    sc.get
  }

  def stop(): Unit = {
    if (sc.isDefined) {
      sc.get.stop()
      sc = None
    }
  }
}