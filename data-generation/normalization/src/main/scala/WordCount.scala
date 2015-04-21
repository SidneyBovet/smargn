import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import java.io.PrintWriter

object WordCountPerYear {
  def main(args: Array[String]) {

    if(args.length != 2) {
       println("USAGE: WordCountPerYear inputdir newoutputdir");
       exit(1);
    }

    val spark = new SparkContext(new SparkConf().setAppName("WordCountPerYear"))

    // open the file
    val file = dataFormatter(spark.textFile(args(0)))
    val acc = Array.fill[Int](file.take(1)(0).length)(0)
    val sums = file.fold(acc)((a,e) => a.zip(e).map(el=>el._1+el._2))

    // Write cleaned 1grams on HDFS
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(args(1)))
    sums.foreach(el => out.write((el.toString+",").getBytes))
    out.close()
    fs.close()

    spark.stop()
  }

  def dataFormatter(data: RDD[(String)]): RDD[Array[Int]] = {
    return data.map(line => line.split(" ")).map((i: Array[String]) => i.tail.map(y => y.toInt))
  }
}

