package xyz.hakula;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndex {
  private static final String DELIM = ":";

  public static class TokenMapper extends Mapper<Object, Text, Text, Text> {
    private final Text key = new Text();
    private final Text count = new Text("1");

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      var split = (FileSplit) context.getInputSplit();
      var it = new StringTokenizer(value.toString());
      while (it.hasMoreTokens()) {
        var filename = split.getPath().getName();
        var token = it.nextToken();
        this.key.set(token + DELIM + filename);
        context.write(this.key, count);
      }
    }
  }

  public static class TokenCountCombiner extends Reducer<Text, Text, Text, Text> {
    private final Text value = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      var count = 0;
      for (var value : values) {
        count += Integer.parseInt(value.toString());
      }
      var items = key.toString().split(DELIM);
      var token = items[0];
      var filename = items[1];
      key.set(token);
      value.set(filename + DELIM + count);
      context.write(key, value);
    }
  }

  public static class TokenCountReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      StringBuilder countList = new StringBuilder();
      for (var value : values) {
        countList.append("(").append(value).append(") ");
      }
      result.set(countList.toString());
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenMapper.class);
    job.setCombinerClass(TokenCountCombiner.class);
    job.setReducerClass(TokenCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
