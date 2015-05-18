package ch.epfl.bigdata15.ngrams.parsing;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Write a file for each year at least a word occur.
 * Each file start with the year and then is a list of word that appear
 * this year separated by space (with repetition)
 * 
 * @author Zhivka Gucevska & Florian Junker
 */
public class SSVOutputFormat extends FileOutputFormat<Text, Text> {
    public SSVRecordWriter getRecordWriter(TaskAttemptContext context) {
        return new SSVRecordWriter(context);
    }

    public class SSVRecordWriter extends RecordWriter<Text, Text> {

        TaskAttemptContext context;

        public SSVRecordWriter(TaskAttemptContext context) {
            this.context = context;
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
        }

        @Override
        public void write(Text key, Text value) throws IOException,
                InterruptedException {
            Path outputPath = FileOutputFormat.getOutputPath(context);
            Path filePath = new Path(outputPath, new Path(key.toString()));
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
            FSDataOutputStream out = fs.create(filePath, context);
            out.write((key.toString() + " " + value.toString()).getBytes("UTF-8"));

            out.close();
            fs.close();
        }

    }
}
