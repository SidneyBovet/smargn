#!/bin/sh

echo "Building..."

sbt package &&

echo "Launching Scala Spark job..." &&

spark-submit --class Filtering --master spark://sidney-mint:7077 --num-executors 1 target/scala-2.10/filtering_2.10-1.0.jar filter.csv out &&

cat out/*

rm -r out/
