package ch.epfl.bigdata15.ngrams.parsing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ArticleReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int size = 0;
		for (Text val : values) {
			size += 1 + val.getLength();
		}
		
		StringBuilder result = new StringBuilder(size);
		for (Text val : values) {
			result.append(" " + val.toString());
		}

		context.write(key, new Text(result.substring(1)));
	}
}
