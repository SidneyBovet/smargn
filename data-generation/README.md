# Data generation #


## General ##

This folder contains three sub-projects, some using spark and some using mapreduce. The justification for the technology used are stated by the owner of the respective sub-projects in ```subfolder/README.md```.

The owners of the sub-projects are:

|            Subproject            |      Owners      |
| -------------------------------- | ---------------- |
| parsing                          | Florian & Zhivka |
| 1gram-generation _aka MapReduce_ | John & Fabien    |
| 1gram-cleaning                   | Florian & Sidney |


## Launching the sub-projects ##

### Parsing
```bash
$ cd parsing
$ mvn package
$ hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ParseDriver <input dir> <output dir>
```

### 1gram generation _aka MapReduce_
_TO BE CORRECTED BY OWNERS_
```bash
$ cd 1gram-generation
$ ./jar.sh
$ ./jar2.sh
```

### 1gram-cleaning
```bash
$ cd 1gram-cleaning
$ sbt package
$ spark-submit --class 1gram-cleaning --master yarn-client --num-executor X path/to/jar
```

### All projects
```bash
$ ./run-all.sh
```
