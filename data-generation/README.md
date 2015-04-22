# Data generation #


## General ##

This folder contains three sub-projects, some using spark and some using
mapreduce.

The owners of the sub-projects are:

|            Subproject            |      Owners      |
| -------------------------------- | ---------------- |
| parsing                          | Florian & Zhivka |
| 1gram-generation _aka MapReduce_ | John & Fabien    |
| 1gram-cleaning                   | Florian & Sidney |


## Launching the sub-projects ##

### Parsing
```bash
cd parsing
mvn package
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ch.epfl.bigdata15.ngrams.parsing.ParseDriver <input dir> <output dir>
```

### 1gram generation _aka MapReduce_
_Note: The first jar must be run twice (once for each newspaper). Then the 2 output folders must be merged into an input folder for the second jar._
```bash
cd 1gram-generation/step1
mvn package
hadoop jar target/1gram-step1-0.0.1-SNAPSHOT.jar mapred.MapReduce <input dir> <output dir>
cd ../step2
mvn package
hadoop jar target/1gram-step2-0.0.1-SNAPSHOT.jar mapred.MapReduce2 <input dir> <output dir>

```

### 1gram-cleaning
```bash
cd 1gram-cleaning
sbt package
spark-submit --class OneGramCleaning --master yarn-client --num-executors <numExecutors> target/scala-2.10/onegramcleaning_2.10-1.0.jar <input dir> <output dir> <sample input dir> <sample output dir> <threshold>
```

### All projects
```bash
./run-all.sh clean #the clean is optional, if you want to clean the build paths
```