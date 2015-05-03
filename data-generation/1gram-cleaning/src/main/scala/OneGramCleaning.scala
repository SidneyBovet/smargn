import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream

object OneGramCleaning {
  def main(args: Array[String]) {
    if(args.length < 3) {
      print("You must provide at least three arguments, inputfile path and outputfile path, profil outputfile path")
      System.exit(1)
    }
    // threshold value for words
    val minOcc = if(args.length >= 6) args(5).toInt else 100

    val sc = new SparkContext(new SparkConf().setAppName("OneGramCleaning"))
    val lines = sc.textFile(args(0))
    val parsedLines = lines.map(line => {
      val splittedLine = line.split("\\s+")
      (splittedLine.head,splittedLine.tail.map(_.toInt))
    })

    // array of lines that occure more that minOcc
    val filteredLines = parsedLines.filter(_._2.sum > minOcc)

    // sum over each year
    val yearCount = filteredLines.take(1)(0)._2.length
    val acc = Array.fill[Int](yearCount)(0)
    val sumsPerYeas = filteredLines.map(_._2).fold(acc)((a,e) => a.zip(e).map(el=>el._1+el._2))

    /*
    val normalizedLines = filteredLines.map(el =>
      (el._1, el._2.zip(sumsPerYeas).map(el => el._1.toDouble/el._2.toDouble)))*/

    // Write cleaned 1grams on HDFS
    formatLine(filteredLines).saveAsTextFile(args(1))
    if(args.length >= 7) {
      val garbageLines = parsedLines.filter(x=> x._2.sum <= minOcc && x._2.sum > minOcc-10)
      formatLine(garbageLines).saveAsTextFile(args(6))
    }

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(args(2)))
    out.write(sumsPerYeas.mkString(" ").getBytes("UTF-8"))
    out.close()
    fs.close()


    //Compute and write samples
    if(args.length >= 5) {
      val sampleList = sc.textFile(args(3)).flatMap(_.split("\\s+")).collect
      val samples = filteredLines.filter(x => sampleList.contains(x._1))
      formatLine(samples).saveAsTextFile(args(4))
    }

    sc.stop()
  }

  def formatLine(rdd:RDD[(String,Array[Int])]): RDD[String] = rdd.map(x=> x._1+" "+x._2.mkString(" "))
}
