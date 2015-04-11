#!/bin/sh

#TODO: remove all output folders

## parsing ##
cd parsing
mvn package &&
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ParseDriver <input dir> <output dir> &&
cd .. &&

## 1gram generation aka MapReduce ##
#TO BE CORRECTED BY OWNERS
cd 1gram-generation &&
./jar.sh &&
./jar2.sh &&
cd .. &&

## 1gram-cleaning ##
cd 1gram-cleaning &&
sbt package &&
hadoop fs -rm /projects/temporal-profiles/clean-1gram &&
spark-submit --class OneGramCleaning --master yarn-client --num-executor 20 target/THEJAR.jar input output
