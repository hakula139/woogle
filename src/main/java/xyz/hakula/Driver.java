package xyz.hakula;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(LongWritable.class);
    job1.setReducerClass(TokenPosition.Reduce.class);
    job1.setNumReduceTasks(NUM_REDUCE_TASKS);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, tempPath);
    if (!job1.waitForCompletion(true)) System.exit(1);

    return 0;
  }
}
