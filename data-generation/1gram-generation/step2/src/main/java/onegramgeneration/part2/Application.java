package onegramgeneration.part2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.Collections;

public class Application {
  // Type the Map receive || Type output of Map
  public static class Map extends Mapper<Object, Text, Text, Text> {


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String keyIn = (value.toString()).split("\\s+")[0];

      String valueIn = (value.toString()).split("\\s+")[1];

      String keyInYear = keyIn.split("_")[0];

      String keyInWord = keyIn.split("_")[1];
      
      context.write(new Text(keyInWord), new Text(keyInYear + "_" + valueIn));
    }
  }

  public static class MyReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> yearCounter = new ArrayList<String>();
      String result = "";

      for (Text val : values) {
        yearCounter.add(val.toString());
      }
      Collections.sort(yearCounter);
      ArrayList<String> listFinal = new ArrayList<String>();

      int base = 1840;
      int rootYear = 1840;
      int lastyear = 1999;
      for (int i = 0; i < yearCounter.size(); i++) {
        int existingYear = Integer.parseInt(yearCounter.get(i).split("_")[0]);
        int missingNumYear = existingYear - rootYear;
        if (missingNumYear == 0) {
          listFinal.add(rootYear - base, yearCounter.get(i));
          rootYear += 1;
        } else {
          for (int j = 0; j < missingNumYear; j++) {
            listFinal.add(rootYear - base, "0");
            rootYear += 1;
          }
          listFinal.add(rootYear - base, yearCounter.get(i));
          rootYear += 1;
        }
      }
      int tempsize = listFinal.size();
      for (int i = listFinal.size(); i <= tempsize + lastyear - rootYear; i++) {
        listFinal.add(i, "0");
      }

      for (String s : listFinal) {
        if (!s.equals("0")) {
          s = s.split("_")[1];
        }
        result += s + " ";
      }

      context.write(key, new Text(result));
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "1-gram generation part 2");
    job.setJarByClass(Application.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}
