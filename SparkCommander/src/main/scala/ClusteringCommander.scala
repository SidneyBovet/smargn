import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.{SparkContext, SparkConf}
import techniques.DynamicTimeWrapping
import utils.Formatting._
import utils.HDFSHandler

/**
 * Created by ana on 18.5.15.
 */
object ClusteringCommander {
  type Metric = (Array[Double], Array[Double], List[Double]) => Double


  val INPUT = "hdfs:///projects/temporal-profiles/data-generation/clean-1gram"
  val BASE_PROFILE = "hdfs:///projects/temporal-profiles/data-generation/baseProfile"


  def runClustering(inputDir: String, baseProfileFile: String, spark: SparkContext, k: Int): Unit = {
    // val logger = Logger.getLogger(Launcher.getClass.getName)
    val tech = "dtwScaled"
    val metric: Metric = DynamicTimeWrapping.dtwMetricScaleAvg
    val params = List[Double]()


    val outputDir = "hdfs:///projects/temporal-profiles/results/clustering"
    val fileName = s"/${k}clusters${tech}.txt"
    val resp = new Path(outputDir + fileName)
    val hdfs = new HDFSHandler(spark.hadoopConfiguration)
    hdfs.appendToFile(resp)(List("number of cluster -> words"))
    hdfs.close



    val data = spark.textFile(inputDir)
    val baseProfile = spark.textFile(baseProfileFile).take(1)(0).split(" ").map(_.toInt)
    //Formatting part
    val formattedData = dataFormatter(data, baseProfile).cache()

    val zip = formattedData.zipWithUniqueId().cache()
    val productPairs = zip.cartesian(zip).filter(x => x._1._2 < x._2._2).cache()

    val trainingSet = productPairs.map(x => (x._1._2, x._2._2, 1 - metric(x._1._1._2, x._2._1._2, params)))

    println("training Set Created")

    val maxIterations = 100000

    val model = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(maxIterations)
      .run(trainingSet)

    println("model built")

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }

    def IdtoWord(id: Long): String = {
      zip.filter(x => x._2 == id).first()._1._1
    }

    val assignmentsStr = assignments
      .map { case (k, v) =>
      s"$k -> ${v.map(l => IdtoWord(l)).mkString("[", ",", "]")}"
    }.mkString(",")
    val sizesStr = assignments.map {
      _._2.size
    }.sorted.mkString("\n")



    val hdfs1 = new HDFSHandler(spark.hadoopConfiguration)
    hdfs1.appendToFile(resp)(List(assignmentsStr))
    hdfs1.close()


    println("OK")

  }


  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("ClusteringCommander").setMaster("yarn-cluster").set("num-executors", "25")

    @transient val sc = new SparkContext(conf)

    runClustering(INPUT, BASE_PROFILE, sc, 5000)
    sc.stop()


  }
}
