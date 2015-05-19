package ch.epfl.bigdata15.ngrams.parsing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce all word from the same year together
 * And make a string of then separated by spaces
 * 
 * @author Zhivka Gucevska & Florian Junker
 */
public class ArticleReducer extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
    throws IOException, InterruptedException {
	
	// Use of string builder since we have a lot of words to concatenate and it is more efficient that String + String 
    StringBuilder result = new StringBuilder();
    for (Text val : values) {
      result.append(" " + val.toString());
    }

    context.write(key, new Text(result.substring(1)));
  }
}