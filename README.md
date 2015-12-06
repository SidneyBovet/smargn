# smargn : reversing the ngrams search process#

##EPFL Big Data course - Backwards search on ngrams##

The goal of this project is to find related words based on the number of occurrences they were used over the years. We make use of Scala spark and Hadoop on a production cluster to perform the required computation.

We use data from Le Temps: 160 years of OCR'd articles, from which we extracted temporal profiles as one can see on Google's Ngram viewer https://books.google.com/ngrams

Then we use dynamic time wrapping, peak detection and other simpler comparison algorithms in order to find similar profiles, yielding interesting results. For instance the word "snow" is associated with "accidents", and "victims", and the word "fights" with "army", "planes", and "Germany" (all with peaks during the two world war).