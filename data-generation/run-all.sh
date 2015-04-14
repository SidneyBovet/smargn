#!/bin/sh

#TODO: remove all output folders

## parsing ##
cd parsing
mvn package &&
hadoop fs -rm -r /projects/temporal-profiles/data-generation/parsing &&
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ch.epfl.bigdata15.ngrams.parsing.ParseDriver /projects/temporal-profiles/data-generation/input /projects/temporal-profiles/data-generation/parsing &&
cd .. &&

## 1gram generation ##
cd 1gram-generation/step1
mvn package &&
hadoop fs -rm -r /projects/temporal-profiles/data-generation/1gram-generation/step1 &&
hadoop jar target/1gram-step1-0.0.1-SNAPSHOT.jar mapred.MapReduce /projects/temporal-profiles/data-generation/parsing /projects/temporal-profiles/data-generation/1gram-generation/step1 &&
cd ../step2 &&
mvn package &&
hadoop fs -rm -r /projects/temporal-profiles/data-generation/1gram-generation/step2 &&
hadoop jar target/1gram-step2-0.0.1-SNAPSHOT.jar mapred.MapReduce2 /projects/temporal-profiles/data-generation/1gram-generation/step1 /projects/temporal-profiles/data-generation/1gram-generation/step2 &&
cd ../.. &&

## 1gram-cleaning ##
cd 1gram-cleaning &&
sbt package &&
hadoop fs -rm /projects/temporal-profiles/data-generation/clean-1gram &&
spark-submit --class OneGramCleaning --master yarn-client --num-executor 20 target/scala-2.10/onegramcleaning_2.10-1.0.jar "hdfs:///projects/temporal-profiles/data-generation/1gram-generation/step2" "hdfs:///projects/temporal-profiles/data-generation/clean-1gram" 
