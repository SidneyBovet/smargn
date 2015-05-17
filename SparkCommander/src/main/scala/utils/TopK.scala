package utils

import org.apache.spark.rdd.RDD
import utils.Launcher.Metric

import scala.collection.mutable

/**
 * Created by Joanna on 4/26/15.
 * @author Mathieu Monney & Joanna Salathé
 *         Helper for retrieving TopK word
 */
object TopK {

  /**
   * order the similar words based on the result of the metric in a tree of size k and return its elements
   * @param k number of similar words we want to retrieve
   * @param metric metric used to order the similar words
   * @param data data collection of words with their frequency
   * @param testedWord word that we want to find its similar words
   * @param order the ordering used to order the words
   * @return top k most similar words to the testedWord
   */
  def retrieveTopK(k: Integer, metric: Metric, data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]),
                   order: ((String, Double), (String, Double)) => Boolean,
                   metricParameters: List[Double] = List()): Array[String] = {

    val myOrdering = Ordering.fromLessThan[(String, Double)](order)
    val dataMetric = data.map(x => (x._1, metric(x._2, testedWord._2, metricParameters))).cache()

    // Specify that the mapPartition operation will be executed on each partition separately
    val keepPartitions = true

    // For each partition, returns the k words that have the higher ranking
    val partialTopK = dataMetric.mapPartitions(it => {
      var i = 0
      val tree = mutable.TreeSet.empty(myOrdering)
      while (it.hasNext && i < k) {
        val current = it.next()
        if (current._1 != testedWord._1) {
          tree.add(it.next())
          i += 1
        }
      }
      while (it.hasNext) {
        val elem = it.next()
        if (elem._1 != testedWord._1) {
          val last = tree.last
          if (elem._2 < last._2) {
            tree.remove(last)
            tree.add(elem)
          }
        }
      }
      tree.iterator
    }, keepPartitions)

    // Collect the results of each partition and take the k best elements
    partialTopK.takeOrdered(k)(myOrdering).sorted(myOrdering).map(_._1)
  }
}

