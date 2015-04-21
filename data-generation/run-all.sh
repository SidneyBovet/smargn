#!/bin/sh

echo "Starting the whole data generation pipeline..."

## parsing ##
cd parsing &&
mvn $1 package &&
hadoop fs -rm -r /projects/temporal-profiles/data-generation/parsing ;
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ch.epfl.bigdata15.ngrams.parsing.ParseDriver /projects/temporal-profiles/data-generation/input /projects/temporal-profiles/data-generation/parsing &&
cd .. &&

## 1gram generation ##
cd 1gram-generation/step1 &&
mvn $1 package &&
hadoop fs -rm -r /projects/temporal-profiles/data-generation/1gram-generation/step1 ;
hadoop jar target/part1-0.0.1-SNAPSHOT.jar onegramgeneration.part1.Application /projects/temporal-profiles/data-generation/parsing /projects/temporal-profiles/data-generation/1gram-generation/step1 &&
cd ../step2 &&
mvn package &&
hadoop fs -rm -r /projects/temporal-profiles/data-generation/1gram-generation/step2 ;
hadoop jar target/part2-0.0.1-SNAPSHOT.jar onegramgeneration.part2.Application /projects/temporal-profiles/data-generation/1gram-generation/step1 /projects/temporal-profiles/data-generation/1gram-generation/step2 &&
cd ../.. &&

## 1gram-cleaning ##
cd 1gram-cleaning &&
sbt $1 package &&
hadoop fs -rm /projects/temporal-profiles/data-generation/clean-1gram ;
spark-submit --class OneGramCleaning --master yarn-client --num-executors 20 target/scala-2.10/onegramcleaning_2.10-1.0.jar "hdfs:///projects/temporal-profiles/data-generation/1gram-generation/step2" "hdfs:///projects/temporal-profiles/data-generation/clean-1gram" "hdfs:///projects/temporal-profiles/data-generation/sample/sampleList" "hdfs:///projects/temporal-profiles/data-generation/sample/samples" &&
echo "------------------------------" &&
echo "Pipeline successfully achieved" &&
echo "------------------------------"

echo -n "Attempting to change rights on the output folder..."
hadoop fs -chgrp -R bigdata2015-temporalprofiles /projects/temporal-profiles/data-generation/ &&
hadoop fs -chmod -R g+w /projects/temporal-profiles/data-generation/ &&
echo " done."
