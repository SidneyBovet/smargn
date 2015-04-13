# Data generation #


## General ##

This folder contains three sub-projects, some using spark and some using
mapreduce. The justification for the technology used are stated by the owner of
the respective sub-projects in ```subfolder/README.md```.

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
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ParseDriver <input dir> <output dir>
```

### 1gram generation _aka MapReduce_
_Note: The first jar must be run twice (once for each newspaper). Then the 2 output folders must be merged into an input folder for the second jar._
```bash
cd 1gram-generation
./jar.sh
./jar2.sh
```

### 1gram-cleaning
```bash
cd 1gram-cleaning
sbt package
hadoop fs -rm /projects/temporal-profiles/clean-1gram
spark-submit --class OneGramCleaning --master yarn-client --num-executors 25 target/scala-2.10/onegramcleaning_2.10-1.0.jar "hdfs:///projects/temporal-profiles/mapReduce/fullOutput2" "hdfs:///projects/temporal-profiles/clean-1gram"
```

### All projects
```bash
./run-all.sh
```
