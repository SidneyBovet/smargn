import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.Scanner;

public class MapReduce {
    //                                          Type the Map receive || Type output of Map        
    public static class Map extends Mapper<Text, IntWritable, Text, IntWritable> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static class MyReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }    

    public static void main(String[] args) throws Exception {

        // FIRST JOB
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce1 ");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(MyReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TupleInputFormat.class);
        //job.setOutputFormatClass(GZipFileOutputFormat.class);


        TupleInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // SECOND JOB
        /*
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MapReduce2");
        job2.setJarByClass(MapReduce.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(MyReducer2.class);


        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setInputFormatClass(TupleInputFormat.class);


        TupleInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        */

    }

    // Name of file and line
    public static class TupleInputFormat extends FileInputFormat<Text, IntWritable> {

        @Override
        public RecordReader<Text, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new TupleRecordReader();
        }
    }

    /**
     * This RecordReader implementation decompress a GZ file. The "key" 'Text' is the
     * decompressed file name, the "value" 'BytesWritable' is the file contents.
     * Code based on the article http://cotdp.com/2012/07/hadoop-processing-zip-files-in-mapreduce/
     */
    public static class TupleRecordReader extends RecordReader<Text, IntWritable> {

        /** Uncompressed file name */
        private Text currentKey;

        /** Uncompressed file contents */
        private IntWritable currentValue;

        /** Used to indicate progress */
        private boolean isFinished = false;

        private String year;
        private final static IntWritable ONE = new IntWritable(1);
        private Scanner input;

        /**
         * Initialise and open the GZ file from the FileSystem
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
                InterruptedException {

            FileSplit split = (FileSplit) inputSplit;
            Configuration conf = taskAttemptContext.getConfiguration();
            Path path = split.getPath();

            FileSystem fs = path.getFileSystem(conf);
            // Open the stream
            input = new Scanner(fs.open(path));
            year = input.next();

        }

        /**
         * Decompress the Gzip entry.
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          if(input.hasNext()){
          currentKey =  new Text(year+"_"+input.next());
          currentValue = ONE;
          return true;  
          }
          isFinished = true;
          return false;
        }

        /**
         * Rather than calculating progress, we just keep it simple
         */
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return isFinished ? 1 : 0;
        }

        /**
         * Returns the current key (name of the GZ file)
         */
        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        /**
         * Returns the current value (contents of the GZ file)
         */
        @Override
        public IntWritable getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        /**
         * Close quietly, ignoring any exceptions
         */
        @Override
        public void close() throws IOException {
            try {
                input.close();
            } catch (Exception ignore) {
                System.out.println("Error");
            }
        }
    }

    /**
     * Extends the basic FileOutputFormat class provided by Apache Hadoop to
     * write the file in the wanted format.
     */
    public static class GZipFileOutputFormat extends FileOutputFormat<Text, BytesWritable> {
        @Override
        public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext job) {
             // create our record writer with the new file
            return new GZipRecordWriter(job);

        }
    }
    /*
     * Code based on the article http://johnnyprogrammer.blogspot.ch/2012/01/custom-file-output-in-hadoop.html
     */
    public static class GZipRecordWriter extends RecordWriter<Text, BytesWritable> {
        Path path = null;
        Path fullPath = null;
        FileSystem fs = null;
        FSDataOutputStream fileOut = null;
        DataOutputStream out = null;
        TaskAttemptContext job = null;

        public GZipRecordWriter(TaskAttemptContext job) {
            this.job = job;
            path = FileOutputFormat.getOutputPath(job);
        }

        @Override
        public void close(TaskAttemptContext job) throws IOException, InterruptedException {
            out.close();
        }

        @Override
        public void write(Text key, BytesWritable value) throws IOException, InterruptedException {
            if(key != null && value != null){
                String keyString = key.toString();
                // Remove .gz extension in file name
                String cleanedKeyString = keyString.substring(0,keyString.length()-3);
                fullPath = new Path(path,cleanedKeyString);
                // Create the file in the file system
                try {
                    fs = path.getFileSystem(job.getConfiguration());
                    fileOut = fs.create(fullPath, job);
                    out = fileOut;
                } catch (Exception ignore) {
                    System.out.println("Error");
                }
                // Write out our 'value'
                fileOut.write(value.getBytes(),0,value.getLength());
            }
        }
    }

}
