#!/bin/sh

echo "Starting the whole data generation pipeline..."

inputPath="/projects/temporal-profiles/data-generation/input"
parsingPath="/projects/temporal-profiles/data-generation/parsing"
generationStep1="/projects/temporal-profiles/data-generation/1gram-generation/step1"
generationStep2="/projects/temporal-profiles/data-generation/1gram-generation/step2"
baseProfile="/projects/temporal-profiles/data-generation/baseProfile"
cleanGram="/projects/temporal-profiles/data-generation/clean-1gram"
samples="/projects/temporal-profiles/data-generation/sample/samples"
samplesList="/projects/temporal-profiles/data-generation/sample/sampleList"

## parsing ##
cd parsing &&
mvn $1 package &&
hadoop fs -rm -r  $parsingPath ;
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ch.epfl.bigdata15.ngrams.parsing.ParseDriver $inputPath $parsingPath &&
cd .. &&

## 1gram generation ##
cd 1gram-generation/step1 &&
mvn $1 package &&
hadoop fs -rm -r $generationStep1 ;
hadoop jar target/part1-0.0.1-SNAPSHOT.jar onegramgeneration.part1.Application $parsingPath $generationStep1 &&
cd ../step2 &&
mvn package &&
hadoop fs -rm -r $generationStep2 ;
hadoop jar target/part2-0.0.1-SNAPSHOT.jar onegramgeneration.part2.Application $generationStep1 $generationStep2 &&
cd ../.. &&

## 1gram-cleaning ##
cd 1gram-cleaning &&
sbt $1 package &&
hadoop fs -rm -r $baseProfile ;
hadoop fs -rm -r $cleanGram ;
hadoop fs -rm -r $samples ;
spark-submit --class OneGramCleaning --master yarn-client --num-executors 20 --driver-memory 16g --executor-memory 8g target/scala-2.10/onegramcleaning_2.10-1.0.jar hdfs://$generationStep2 hdfs://$cleanGram hdfs://$baseProfile hdfs://$samplesList hdfs://$samples &&
echo "------------------------------" &&
echo "Pipeline successfully achieved" &&
echo "------------------------------"

echo -n "Attempting to change rights on the output folder..."
hadoop fs -chgrp -R bigdata2015-temporalprofiles /projects/temporal-profiles/data-generation/ &&
hadoop fs -chmod -R g+w /projects/temporal-profiles/data-generation/ &&
echo " done."
