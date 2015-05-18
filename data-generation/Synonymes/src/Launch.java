import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @author Florian
 * This program is to be run offline.
 * It is use to generate a list of synonyms for our words. Since we did not fount one to download we query a website and parse the result.
 * There is no parameter but the program need to have a file named "input" next to the jar
 * The input file must contain a list of word separated by '\n'. In addition everything after a space will be ignore until a return line.
 */
public class Launch {
   
    public static void main(String[] args) throws IOException {
    	// Change the properties of the web agent as if we were google chrome ;-)
    	System.setProperty("http.agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.29 Safari/537.36");

    	InputStreamReader in = new InputStreamReader(new FileInputStream(new File("input")), "UTF-8");
		PrintStream out = new PrintStream(new File("synonymes"));
		Scanner sc = new Scanner(in);
		
		long counter = 0;
		int error = 0;
		boolean redo = false;
		String word = "";
		
		// For each word we retrieve its list of synonyms
		while(sc.hasNext()) {
	        try {
	        	// If previous word didn't failed retrieve the next one
	        	if(!redo)
	        		word = takeWhile(sc.nextLine(), ' ');
	        	
	        	// Fetch synonyms for the current word
		        String synonymes = fetchPage(word);
		        
		        // Write the result in the file
		        out.write((word+","+synonymes+"\n").getBytes("UTF-8"));
		        
		        // A simple terminal interface to show that we are doing some progress
				counter++;
		        if(counter%1000 == 0)
		        	System.out.println(counter);
		        error = 0;
				redo = false;
			} catch (Exception e) {
				// If fetch of synonyms failed then attempt to retry except if we try more than 30 times.
				// In this case we give up
				error++;
				redo = true;
				if(error > 30) {
					redo = false;
					error = 0;
					System.out.println("Failed with word : "+word);
				}
			}
		}
		
		in.close();
		out.close();
    }
    
    /**
     * Parse the webpage
     * @param word that we search synonyms for
     * @return a string representing a list a synonyms separated by a comma
     * @throws IOException
     */
    private static String fetchPage(String word) throws IOException {
    	String url = "http://www.synonymes.com/synonyme.php?mot="+word;

    	HashSet<String> words = new HashSet<String>(); 
        Document doc = Jsoup.connect(url).get();
        Elements list = doc.select("a[href^=synonyme.php?mot=]");
        Iterator<Element> iterator = list.iterator();
        
        while(iterator.hasNext()) {
        	words.add(iterator.next().text());
        }
        
        return mkString(words);
    }
    
    /**
     * This function is similar to its scala homonym with separation char fixed to a comma.
     * It takes a collection of String and return one string with each element separated by a comma
     * @param strings is the collection to convert in one string
     * @return one string representing the all collection. Each element separated by a comma
     */
    private static String mkString(Collection<String> strings) {
    	StringBuilder result = new StringBuilder();
    	for(String str : strings) {
    		if(str.length()<1) continue;
    		result.append(str);
    		result.append(',');
    	}
    	if(result.length() < 1)
    		return "";
    	return result.substring(0, result.length()-1);
    }
    
    /**
     * This function is similar to its scala homonym.
     * It takes a string and return the beginning of it until it reach the character given in parameter
     * @param str is the string we want to cut and only take the beginning
     * @param c is the character that will delemit the end of the string
     * @return return the substring of str until the occurence of c
     */
    private static String takeWhile(String str, char c) {
    	int end = str.length();
    	for (int i = 0; i < str.length(); i++) {
			if(str.charAt(i) == c) {
				end = i;
				break;
			}
		}
    	
    	return str.substring(0, end);
    }
}