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
    val kvPairsNorm = kvPairs.map(t => {
      val meanValue = mean(t._2)
      (t._1, t._2.map(_ / meanValue))
    })
    val words = kvPairsNorm.flatMap(t => {
      if(detectPeaks(t._2,1,1)) {
        List(t._1)
      } else {
        List()
      }
    })

    words.saveAsTextFile(args(1))

    sc.stop()
  }

  /**
    * Returns an RDD[String,Array[Int]] from a CSV file using space as delimiters
    */
  def filenameToKeyValue(fileName: String, sc: SparkContext) = {
    sc.textFile(fileName)
      .map(_.split(" "))
      .keyBy(_.head) // produce (word,[w,f1,f2,...]) tuples
      .map(k => (k._1,k._2.tail.map(_.toInt))) // produce (word, Array[Int])
  }

  /**
    * Returns the mean of array
    */
  def mean(a: Array[Int]): Double = {
    val sum = a.foldLeft(0)(_+_)
    sum/a.size
  }

  /**
    * Returns true if any point in array goes too far from the mean
    */
  def detectPeaks(array: Array[Double], mean: Double, threshold: Double): Boolean = {
    array.foldLeft(false)((b:Boolean,el:Double) => {
      (b || ((el-mean)*(el-mean) > threshold))
    })
  }

}
