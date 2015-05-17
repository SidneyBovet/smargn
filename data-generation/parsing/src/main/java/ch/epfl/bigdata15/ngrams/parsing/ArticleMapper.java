package ch.epfl.bigdata15.ngrams.parsing;

import java.io.IOException;
import java.util.Locale;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArticleMapper extends Mapper<Text, Path, Text, Text> {

    public void map(Text key, Path value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        FileSystem fs = value.getFileSystem(conf);
        FSDataInputStream in = fs.open(value);
        Scanner scan = new Scanner(in, "UTF-8");
        scan.useDelimiter("[\\s+>]");

        String article = readNextArticle(scan);
        while (article.length() > 0) {
            context.write(key, new Text(article));
            article = readNextArticle(scan);
        }
        scan.close();
        in.close();
        fs.close();
    }

    private String readNextArticle(Scanner scan) {
        String article = "";
        String word = "";
        boolean inArticle = false;

        while (scan.hasNext()) {
            word = scan.next();
            if ("<full_text".equals(word)) {
                inArticle = true;
            } else if ("</full_text".equals(word)) {
            	String trimActicle = article.trim();
            	if(trimActicle.length() > 0) {
            		return trimActicle;
            	}
            } else if (inArticle) {
            	word = cleanWord(word);
            	if(word.length() > 1) {
            		article += word.toLowerCase(Locale.FRENCH) + " ";
            	} 
            }
        }

        return article.trim();
    }

    
    /**
     * This method clean word
     * @param word is the string to be clean
     * @return the clean word
     */
    private String cleanWord(String word) {
    	/* All this method should be possible to implement with the following replaceAll however,
    	 * for a reason I did discovered, this doesn't work. (This works outside a Map Reduce)
    	 * word.replaceAll("^['-]*", "").replaceAll("['-]*$", "").replaceAll("[^\\p{L}'-]", "");
    	 */
    	word = word.replaceAll("[^\\p{L}']", "");
    	int newBegin = 0;
    	int newEnd = word.length();
    	while(newBegin < newEnd && (word.charAt(newBegin) == '\'' || word.charAt(newBegin) == '-') ||
    			(newBegin+1 < newEnd && word.charAt(newBegin+1) == '\'')) {    		
		newBegin++;
    	}
    	newEnd--;
    	while(newEnd > newBegin && (word.charAt(newEnd) == '\'' || word.charAt(newEnd) == '-')) {
    		newEnd--;
    	}
    	
    	return word.substring(newBegin, newEnd+1);
    }
}
