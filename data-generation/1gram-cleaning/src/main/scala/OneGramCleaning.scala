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
    if(args.length < 2) {
      print("You must provide at least two arguments, inputfile path and outputfile path")
      System.exit(1)
    }
    // threshold value for words
    val minOcc = if(args.length >= 5) args(4).toInt else 50
    val sc = new SparkContext(new SparkConf().setAppName("OneGramCleaning"))
    val lines = sc.textFile(args(0))
    val parsedLines = lines.map(line => {
      val splittedLine = line.split("\\s+")
      (splittedLine.head,splittedLine.tail.map(_.toInt))
    })

    // array of lines that occure more that minOcc
    //val filteredLines = sumAndLine.filter(_._1 > minOcc).map(_._2).map(_+"\n").collect
    val filteredLines = parsedLines.filter(_._2.sum > minOcc)

    // sum over each year
    val yearCount = filteredLines.take(1)(0)._2.length
    val acc = Array.fill[Int](yearCount)(0)
    val sumsPerYeas = filteredLines.map(_._2).fold(acc)((a,e) => a.zip(e).map(el=>el._1+el._2))

    val normalizedLines = filteredLines.map(el =>
      (el._1, el._2.zip(sumsPerYeas).map(el => el._1.toDouble/el._2.toDouble)))

    // Write cleaned 1grams on HDFS
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = fs.create(new Path(args(1)))
    printArray(yearCount,normalizedLines.collect,out)
    out.close()

    //Compute and write samples
    if(args.length >= 4) {
      val sampleList = sc.textFile(args(2)).flatMap(_.split("\\s+")).collect
      val samples = normalizedLines.filter(x => sampleList.contains(x._1))
      val out = fs.create(new Path(args(3)))
      printArray(yearCount,samples.collect,out)
      out.close()
    }

    fs.close()
    sc.stop()
  }

  def printArray(numYears:Int,a:Array[(String,Array[Double])], out:FSDataOutputStream) = {
    out.writeInt(numYears)
    a.foreach(el => {
      val wordBytes = el._1.getBytes("UTF-8")
      out.writeInt(wordBytes.length)
      out.write(wordBytes)
      el._2.foreach(freq => out.writeDouble((freq)))
      })
  }
}
