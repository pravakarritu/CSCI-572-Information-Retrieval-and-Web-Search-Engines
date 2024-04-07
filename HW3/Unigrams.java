import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Unigrams {
  public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text docid = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9 ]+", " "));
      // String name = ((FileSplit)context.getInputSplit()).getPath().toString();

      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String fileName = fileSplit.getPath().getName().split("[.]")[0];

      docid.set(fileName);

      // System.out.println("******************"+value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());

        context.write(word, docid);
      }
    }
  }

  public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      for (Text val : values) {
        String docid = val.toString();
        if (map.containsKey(docid)) {
          map.put(docid, map.get(docid) + 1);
        } else {
          map.put(docid, 1);
        }
      }
      // result.set(sum);
      // context.write(key, result);
      String count = " ";
      for (Map.Entry<String, Integer> e : map.entrySet()) {
        count += e.getKey() + ":" + e.getValue() + " ";
        // System.out.println("Key: " + e.getKey()+ " Value: " + e.getValue());
      }

      result.set(count);
      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Unigrams");

    job.setJarByClass(Unigrams.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

