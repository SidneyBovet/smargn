import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.PrintWriter

object WordCountPerYear {
  def main(args: Array[String]) {

    if(args.length != 2) {
       println("USAGE: WordCountPerYear inputdir newoutputdir");
       exit(1);
    }

    val spark = new SparkContext(new SparkConf().setAppName("WordCountPerYear"))

    // open the files
    val files = spark.textFile(args(0))

    val counts = files.map(line => {
      val words = line.split(" ")
      val year = words(0).toInt
      (year,words.length - 1)
    })
    val finalArray = counts.sortBy(_._1).map(_._2).collect

    // Write cleaned 1grams on HDFS
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(args(1)))
    finalArray.foreach(el => out.write((el.toString+",").getBytes))
    out.close()
    fs.close()

    spark.stop()
  }
}

