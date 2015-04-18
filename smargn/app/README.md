# App folder: core of our project #

## General ##

The main components of the general architecture are the following:
-------------------------------------------------------------------------------------------------------------------------------------------|
|           Components            |                                 Purpose                                                                |
|---------------------------------|--------------------------------------------------------------------------------------------------------|
| controllers                     | contains: - Application (main link between web page and our scala code)                                |
|                                 |           - Spark (use to initialize sparkContext for the whole app)                                   |
| techniques                      | contains: all the techniques to find similar words for a given word                                    |
| utils                           | contains: - ConputationUtilies (functions for common computation [e.g. average, variance...]           |
|                                 |           - Filtering (functions for detecting interesting temporal profiles)                          |
|                                 |           - Formatting (functions for formatting the data)                                             |
|                                 |           - Grapher (functions used to display graph)                                                  |
|                                 |           - Launcher (called by Application, process that take a word                                  |
|                                 |             , use a similarity function on it and display the result)                                  |
|                                 |           - Scaling (functions for scaling curves before applying a similarity technique)              |
|                                 |           - SubTechniques (other techniques for changing curves before applying a similarity technique)|
| views                           | contains: HTML pages displayed by our app                                                              |
-------------------------------------------------------------------------------------------------------------------------------------------|

## Launcher idea ##

The Launcher is the backbone of the similarity function process. Basically it works as follow:
Format data and words => apply similarity technique => get the similar words from the data => display the output

its run method is called by the Application in the following way:
run(word, "input/", "public/data/", PARAMETERS, SIMILARITY_TECHNIQUE)

- word will be taken from the web page
- input/ is to be created at the same level as the app/ folder and must contain the output of the wordCount (namely a file of the format "word occ1 occ2 [...] occn" per line)
- public/data/ is the output folder
- PARAMETERS is of the type List[Double] and will contain the parameters that the SIMILARITY_TECHNIQUE will use (/!\ order of the parameters must be the same as used in the technique)
- SIMILARITY_TECHNIQUE is the technique chosen to be use

## Similarity technique idea ##

Each techniques must follow the given signature in order to be used by the Launcher: 
input: (data: RDD[(String, Array[Double])], testedWord: (String, Array[Double]), parameters: List[Double])
output: RDD[(String)]

---Parameters extraction---
Parameters can be pass to the function via the PARAMETERS list (see prevous paragragh), be very careful of the order

---Output---
Output must contain only the String of the similar word and not its list of frequency. 
The reason is that often the similarity function are changing the frequency list, thus it is needed to be retrieve back by the Launcher afterwards.

---Preprocessing---
You are free to add possibility of applying "pre-techniques" (scaling, smoothing, etc..) before applying your technique.
For that purpose the easiest way if you create a similarity technique X and you want to give the possibility to use the "pre-techniques" A and B before, is to implement in your class X two functions AX and BX that call the pre-techniques before calling the similarity technique.
For example:
NaiveComparisons contains 2 similarty techniques: naiveDifference and naiveDivision
If you want to use the scaling function proportionalScalarMax before calling naiveDifference, use its function naiveDifferenceScalingMax which simply calls the scaling before applying the similarity function

---Utils---
Always check that a utility fonction you want to implement is not already implemented there


