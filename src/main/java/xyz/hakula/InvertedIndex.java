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

public class InvertedIndex {
  public static class WordCountMapper extends Mapper<Object, Text, Text, Text> {
    private Text mapKey = new Text();
    private Text mapValue = new Text();
    private FileSplit words;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      words = (FileSplit) context.getInputSplit();
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {}

  public static void main(String... args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
