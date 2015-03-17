#!/bin/bash

rm MapReduce2.jar
mkdir MapReduce2
javac -classpath ${HADOOP_CLASSPATH} -d MapReduce2/ MapReduce2.java
jar -cvf MapReduce2.jar -C MapReduce2/ .
rm -rf MapReduce2
hadoop fs -rm -r output2
hadoop jar MapReduce2.jar MapReduce2 input2 output2