import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Filtering {
  def main(args: Array[String]) {

    if(args.length != 2) {
      println("USAGE: NaiveFiltering inputfile newoutputdir");
      exit(1);
    }

    val conf = new SparkConf().setAppName("NaiveFiltering")
    val sc = new SparkContext(conf)

    val kvPairs = filenameToKeyValue(args(0),sc).cache
    /*
    val kvPairs = lines.mapPartitionsWithIndex(
      (partitionIdx: Int, lines: Iterator[(String,Array[Int])]) => {
        if (partitionIdx == 0) {
          lines.drop(1)
        }
        lines
      }).cache
     */

    var kvPairsNorm = kvPairs.map(t => (t._1, t._2.map(_ / mean(t._2))))

    /* USELESS
    kvPairsNorm.foreach(t => {
      print("XXXXXXXXXXXXXXXXXXXX (")
      print(t._1)
      print(",[")
      t._2.foreach(el => print(" "+el))
      println(" ])")
    })*/

    kvPairsNorm.saveAsTextFile(args(1))

    sc.stop()
  }

  def filenameToKeyValue(fileName: String, sc: SparkContext) = {
    sc.textFile(fileName)
      .map(_.split(" "))
      .keyBy(_.head) // produce (word,[w,f1,f2,...]) tuples
      .map(k => (k._1,k._2.tail.map(_.toInt))) // produce (word, Array[Int])
  }

  def mean(a: Array[Int]): Double = {
    val sum = a.foldLeft(0)(_+_)
    sum/a.size
  }

  def detectPeaks(array: Array[Int], mean: Int, threshold: Int): Boolean = {
    array.foldLeft(false)((b:Boolean,el:Int) => {
      (b || ((el-mean)*(el-mean) > threshold))
    })
  }

}
