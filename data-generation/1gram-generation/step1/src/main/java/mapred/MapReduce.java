package mapred;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {
  // Type the Map receive || Type output of Map
  public static class Map extends Mapper<Text, IntWritable, Text, IntWritable> {

    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MapReduce1 ");
    job.setJarByClass(MapReduce.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TupleInputFormat.class);

    TupleInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TupleInputFormat extends FileInputFormat<Text, IntWritable> {

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new TupleRecordReader();
    }
  }

  public static class TupleRecordReader extends RecordReader<Text, IntWritable> {

    private Text currentKey;

    private IntWritable currentValue;

    private boolean isFinished = false;

    private String year;
    private final static IntWritable ONE = new IntWritable(1);
    private Scanner input;

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

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (input.hasNext()) {
        currentKey = new Text(year + "_" + input.next());
        currentValue = ONE;
        return true;
      }
      isFinished = true;
      return false;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return isFinished ? 1 : 0;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void close() throws IOException {
      try {
        input.close();
      } catch (Exception ignore) {
        System.out.println("Error");
      }
    }
  }
}
