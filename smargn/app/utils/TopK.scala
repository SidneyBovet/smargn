package utils

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by Joanna on 4/26/15.
 * Helper for retrieving TopK word
 */
object TopK {


  /**
   * order the similar words based on the result of the metric in a tree of size k and return its elements
   * @param k number of similar words we want to retrieve
   * @param metric metric used to order the similar words
   * @param data collection of word, frequency to tuple to look into
   * @param testedWord word that we want to find its similar word
   * @param order the ordering used by the tree
   * @return top k most similar words to the testedWord
   */
  def retrieveTopK(k: Integer, metric: ((String, Array[Double]), (String, Array[Double]), List[Double]) => Double, data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), order: ((String, Double), (String, Double)) => Boolean, metricParameters: List[Double] = List()): List[String] = {

    val myOrdering = Ordering.fromLessThan[(String, Double)](order)
    val tree = mutable.TreeSet.empty(myOrdering)
    val dataMetric = data.map(x => (x._1, metric(x, testedWord, metricParameters))).cache()
    val kElem = dataMetric.take(k)
    kElem.map(tree.add)
    val restElem = dataMetric.zipWithIndex().filter(_._2 >= k).map(_._1)

    restElem.collect().foreach(x => if (x._2 < tree.last._2) {
      tree.remove(tree.last)
      tree.add(x)
    })

    tree.toList.map(_._1)

  }
}

