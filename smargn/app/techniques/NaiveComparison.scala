package techniques

/**
 * Created by Valentin on 17/03/15.
 */
object NaiveComparison extends TechniqueMaster {
  def run(inputDir: String, outputFile: String) = {

      val data = spark.textFile(inputDir)


      //difference value that is accepted to consider two array values similar
      val acceptedDifference = 3;
      //word that we want to have its similar words
      val testedWord = ("parrot", Array(2,3,4,12,3))

      //parse data to have list of value
      val parsedData = data.map(line => line.split(" "))
      //group data in the format (word, Array(list of occurrence))
      val wordOccurrencesData = parsedData.map(i => (i.head, i.tail))
      //add the testedWord values to the arrays for future comparison
      val zipDataTestedWord = wordOccurrencesData.map(x => (x._1, (testedWord._2).zip(x._2)))
      //test similarity criteria between each data word array and the tested word
      val booleanDataTestedWord = zipDataTestedWord.map(x=>(x._1, x._2.map(x=> (x._1.toInt,x._2.toInt)).map(x => ((x._1>x._2)&&(x._1-x._2)<acceptedDifference) || ((x._2>x._1)&&(x._2-x._1)<acceptedDifference))))
      //filter the arrays that have at least one value that didn't pass the similarity test
      val filterSimilarity = booleanDataTestedWord.map(x => (x._1, x._2.filter(!_))).filter(_._2.length == 0).map(x => x._1)

      val res = filterSimilarity.collect

      filterSimilarity.saveAsTextFile(outputFile)

      res.toList
    }
}
