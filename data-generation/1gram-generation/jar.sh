#!/bin/bash

rm MapReduce.jar
mkdir MapReduce
javac -classpath ${HADOOP_CLASSPATH} -d MapReduce/ MapReduce.java
jar -cvf MapReduce.jar -C MapReduce/ .
rm -rf MapReduce
hadoop fs -rm -r output
hadoop jar MapReduce.jar MapReduce input output
