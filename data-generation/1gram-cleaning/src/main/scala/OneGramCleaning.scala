import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.PrintWriter

object OneGramCleaning {
  def main(args: Array[String]) {
  	if(args.length < 2) {
  		print("You must provide at least two arguments, inputfile path and outputfile path")
  		System.exit(1)
  	}
    // threshold value for words
  	val minOcc = if(args.length > 2) args(2).toInt else 50 
    val sc = new SparkContext(new SparkConf().setAppName("OneGramCleaning"))
    val lines = sc.textFile(args(0))
    val sumAndLine = lines.map(line => (line.split("\\s+").tail.map(_.toInt).sum, line))
    // array of lines that occure more that minOcc
    val filteredLines = sumAndLine.filter(_._1 > minOcc).map(_._2).map(_+"\n").collect

    // Write cleaned 1grams on HDFS
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(args(1)))
    filteredLines.foreach(str => out.write(str.getBytes("UTF-8")))
    out.close()
    fs.close()

    sc.stop()
  }
}