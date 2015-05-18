package ch.epfl.bigdata15.ngrams.parsing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This application takes the OCR'd article and output for each year a list of words
 * written this year (with repetition)
 * 
 * @author Zhivka Gucevska & Florian Junker
 */
public class ParseDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ngram parser");
        job.setJarByClass(ParseDriver.class);
        job.setMapperClass(ArticleMapper.class);
        job.setCombinerClass(ArticleReducer.class);
        job.setReducerClass(ArticleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(SSVOutputFormat.class);
        addInputPathRec(conf, job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    /**
     * Add this path to the input.
     * But instead considering it as a single input consider each file as one input.
     * So if given path is a folder all files inside are input and recursively
     * @param conf MapReduce configuration
     * @param job Job to add inputs
     * @param path Path to the file or folder to consider as input recursively
     * @throws IOException
     */
    private static void addInputPathRec(Configuration conf, Job job, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] files = fs.listStatus(path);
        travelSubFolders(files, fs, job);
        fs.close();
    }
    
    /**
     * This function help to travel throw folders recursively
     */
    private static void travelSubFolders(FileStatus[] files, FileSystem fs, Job job) throws IOException {
    	for (FileStatus file : files) {
			if(file.isDirectory()) {
	            FileStatus[] newFiles = fs.listStatus(file.getPath());
	            travelSubFolders(newFiles, fs, job);
			} else {
		        FileInputFormat.addInputPath(job, file.getPath());
			}
		}
    }
}
