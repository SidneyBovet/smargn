# 1gram-cleaning #


## General ##

This folder contains a sub-project of data generation.
This sub-project filter the input data by dropping all words that are considered useless.
A word is considered useless if it occurs less that n times.


### How to use
```bash
sbt package
spark-submit --class OneGramCleaning --master yarn-client --num-executors <numExecutors> target/scala-2.10/onegramcleaning_2.10-1.0.jar <input dir> <output dir> <threshold> <sample input dir> <sample output dir>
```
The first and the second arguments, <input dir> <output dir>, are mandatory. While the three last are optional. However, if you want to generate samples you have to give a threshold value.<br />
<input dir>: path to a file containing 1-gram to process<br />
<output dir>: path to write the resulting 1-gram (it shouldn't exists)<br />
<threshold>: every words that occurs less that this value will be dropped<br />
<sample input dir>: path to a file containing space seperated words that have to be sampled.<br />
<sample output dir>: path to write the resulting sample (it shouldn't exists)<br />
<br />
Note: If a file is on HDFS then the path string should start with "hdfs://"


### Example
```bash
spark-submit --class OneGramCleaning --master yarn-client --num-executors 20 target/scala-2.10/onegramcleaning_2.10-1.0.jar "hdfs:///projects/temporal-profiles/data-generation/1gram-generation/step2" "hdfs:///projects/temporal-profiles/data-generation/clean-1gram"
```

```bash
spark-submit --class OneGramCleaning --master yarn-client --num-executors 20 target/scala-2.10/onegramcleaning_2.10-1.0.jar "hdfs:///projects/temporal-profiles/data-generation/1gram-generation/step2" "hdfs:///projects/temporal-profiles/data-generation/clean-1gram" 30 "hdfs:///projects/temporal-profiles/data-generation/sample/sampleList" "hdfs:///projects/temporal-profiles/data-generation/sample/full_samples"
```
