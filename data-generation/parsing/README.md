# Parsing #


## General ##
The goal of this subproject is to parse the XML files of articles and output a list of word for each year of parution.

For each year at least a word have been written, we output a file with the following structure :
Year word1 word2 word3 ... wordN

Note: wordI and wordJ do not have to be distinct.

### How to use
```bash
mvn package
hadoop jar target/parsing-0.0.1-SNAPSHOT.jar ch.epfl.bigdata15.ngrams.parsing.ParseDriver <input dir> <output dir>
```
