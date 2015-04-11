package ch.epfl.bigdata15.ngrams.parsing;

import java.io.IOException;
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
        scan.useDelimiter("[\\s+]|[>]");

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
                return article.trim();
            } else if (inArticle && word.replaceAll("[^\\p{L}']", "").length() > 0) {
                article += word + " ";
            }
        }

        return article.trim();
    }
}
