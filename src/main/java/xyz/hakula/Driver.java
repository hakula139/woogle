package xyz.hakula;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import xyz.hakula.io.LongArrayWritable;
import xyz.hakula.io.TermFreqWritable;
import xyz.hakula.io.TokenFromFileWritable;
import xyz.hakula.io.TokenPositionArrayWritable;

public class Driver extends Configured implements Tool {
  private static final int NUM_REDUCE_TASKS = 16;
  private static final String TEMP_PATH = "temp";

  private static long totalFileCount;

  public static void main(String[] args) throws Exception {
    var config = new Configuration();
    System.exit(ToolRunner.run(config, new Driver(), args));
  }

  public int run(String[] args) throws Exception {
    var inputPath = new Path(args[0]);
    var tempPath = new Path(TEMP_PATH);
    var outputPath = new Path(args[1]);

    var fs = FileSystem.get(getConf());
    totalFileCount = fs.getContentSummary(inputPath).getFileCount();
    if (fs.exists(tempPath)) fs.delete(tempPath, true);
    if (fs.exists(outputPath)) fs.delete(outputPath, true);

    var job1 = Job.getInstance(getConf(), "token position");
    job1.setJarByClass(TokenPosition.class);
    job1.setMapperClass(TokenPosition.Map.class);
    job1.setMapOutputKeyClass(TokenFromFileWritable.class);
    job1.setMapOutputValueClass(LongWritable.class);
    job1.setReducerClass(TokenPosition.Reduce.class);
    job1.setNumReduceTasks(NUM_REDUCE_TASKS);
    job1.setOutputKeyClass(TokenFromFileWritable.class);
    job1.setOutputValueClass(LongArrayWritable.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, tempPath);
    if (!job1.waitForCompletion(true)) System.exit(1);

    var job2 = Job.getInstance(getConf(), "inverted index");
    job2.setJarByClass(TermFreq.class);
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapperClass(TermFreq.Map.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(TokenPositionArrayWritable.class);
    job2.setReducerClass(TermFreq.Reduce.class);
    job2.setNumReduceTasks(NUM_REDUCE_TASKS);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(TermFreqWritable.class);
    FileInputFormat.addInputPath(job2, tempPath);
    FileOutputFormat.setOutputPath(job2, outputPath);
    if (!job2.waitForCompletion(true)) System.exit(1);

    return 0;
  }
}
