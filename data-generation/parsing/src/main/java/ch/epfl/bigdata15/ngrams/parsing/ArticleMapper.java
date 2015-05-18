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

/**
 * This is the major class of this application
 * It get a file path, read the file, parse the XML and output word in the article
 * 
 * @author Zhivka Gucevska & Florian Junker
 */
public class ArticleMapper extends Mapper<Text, Path, Text, Text> {

    public void map(Text key, Path value, Context context)
            throws IOException, InterruptedException {

    	//read the file
        Configuration conf = context.getConfiguration();
        FileSystem fs = value.getFileSystem(conf);
        FSDataInputStream in = fs.open(value);
        Scanner scan = new Scanner(in, "UTF-8");
        //a word is delimited by spaces or by the end of an XML balise
        scan.useDelimiter("[\\s+>]");

        // Read and parse one word
        String article = readNextArticle(scan);
        while (article.length() > 0) {
        	//push set of (year, word)
            context.write(key, new Text(article));
            // Read and parse one word
            article = readNextArticle(scan);
        }
        scan.close();
        in.close();
        fs.close();
    }

    /**
     * Read and parse one word
     * @param scan the scanner that is reading the file containing the articles
     * @return the next word to push to the reducer or empty string if nothing more to read
     */
    private String readNextArticle(Scanner scan) {
        String article = "";
        String word = "";
        boolean inArticle = false;

        // Read while there is something to read and we didn't find a word
        while (scan.hasNext()) {
            word = scan.next();
            if ("<full_text".equals(word)) {
                //If we are at the beginning of an article we start reading (we will push next words)
                inArticle = true;
            } else if ("</full_text".equals(word)) {
                //End of an article. Then following data is meta data and will be drop until reach begin of next article
            	String trimActicle = article.trim();
            	if(trimActicle.length() > 0) {
            		return trimActicle;
            	}
            } else if (inArticle) {
            	//We find a word. Thus we clean it and push it if it is interesting (long) enough
            	word = cleanWord(word);
            	if(word.length() > 1) {
            		//Every words in lower case to simplify search
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
    	 * word.replaceAll("^['-]*(.'+)*", "").replaceAll("['-]*$", "").replaceAll("[^\\p{L}'-]", "");
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
