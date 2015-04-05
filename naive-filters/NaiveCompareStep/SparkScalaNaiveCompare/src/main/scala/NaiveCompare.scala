import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NaiveCompare {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("naiveCompare")
//                 .setMaster("yarn-cluster")

    val spark = new SparkContext(conf)

    //args(0) is the input directory where the data is
    val data = spark.textFile(args(0))
    
    
    //difference value that is accepted to consider two array values similar
    val acceptedDifference = 3;
    //word that we want to have its similar words
    val testedWord = ("parrot", Array(2,3,4,12,3))

    //parse data to have list of value
    val parsedData = data.map(line => line.split(" "))
    //group data in the format (word, Array(list of occurrence))
    val wordOccurencesData = parsedData.map((i: Array[String]) => (i(0), i.tail))
    //add the testedWord values to the arrays for future comparison
    val zipDataTestedWord = wordOccurencesData.map(x => (x._1, (testedWord._2).zip(x._2)))
    //test similarity criteria between each data word array and the tested word
    val booleanDataTestedWord = zipDataTestedWord.map(x=>(x._1, x._2.map(x=> (x._1.toInt,x._2.toInt)).map(x=> if(((x._1>x._2)&&(x._1-x._2)<acceptedDifference) || ((x._2>x._1)&&(x._2-x._1)<acceptedDifference)) true else false)))
    //filter the arrays that have at least one value that didn't pass the similarity test
    val filterSimilarity = booleanDataTestedWord.map(x => (x._1, x._2.filter(_==false))).filter(_._2.length == 0).map(x => x._1) 
    
    //args(1) is the output directory where the result will be written
    filterSimilarity.saveAsTextFile(args(1))

    spark.stop()
  }
}

