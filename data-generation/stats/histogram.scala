import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.PrintWriter

object Histogram {
  def main(args: Array[String]) {
  	if(args.length < 2) {
  		print("You must provide at least two arguments, inputfile path and outputfile path")
  		System.exit(1)
  	}
    // threshold value for words
  	val minOcc = if(args.length > 2) args(2).toInt else 50 
    val sc = new SparkContext(new SparkConf().setAppName("histogram"))
    val lines = sc.textFile(args(0))
    val sumAndLine = lines.map(line => (line.split("\\s+").tail.map(_.toInt).sum, line)).cache

    val max = sumAndLine.map(_._1).max
    val num = 100;
    val fac: Double = 0.001
    val test = sumAndLine.map(x => (x._1 * fac).toInt).collect
    val histogram = new Array[Int](num)
    test.foreach(x => if(x < num)histogram(x) += 1)
    histogram

    sc.stop()
  }
}