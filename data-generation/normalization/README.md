Yet another sub-project to compute the number of words written each year.

To launch the thing run the following:
```bash
sbt package
spark-submit --class WordCountPerYear --master yarn-client --num-executors 20 target/scala-2.10/sparkscalawordcount_2.10-1.0.jar "hdfs:///projects/temporal-profiles/data-generation/parsing" "hdfs:///projects/temporal-profiles/data-generation/yearWordCount"
```
