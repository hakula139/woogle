package xyz.hakula.index;

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
import xyz.hakula.index.io.*;

import java.io.IOException;

public class Driver extends Configured implements Tool {
  public static final int NUM_REDUCE_TASKS = 128;

  public static void main(String[] args) throws Exception {
    var conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Driver(), args));
  }

  public int run(String[] args) throws Exception {
    var inputPath = new Path(args[0]);
    var outputPath = new Path(args[1]);
    var tempPath = new Path(args[2]);
    var tempPath1 = new Path(tempPath, "output_job1");
    var tempPath2 = new Path(tempPath, "output_job2");
    var fileTokenCountPath = new Path(tempPath, "file_token_count");
    var inverseDocumentFreqPath = new Path(outputPath, "inverse_document_freq");

    var conf = getConf();
    try (var fs = FileSystem.get(conf)) {
      var totalFileCount = fs.getContentSummary(inputPath).getFileCount();
      if (totalFileCount == 0) return 0;
      conf.setLong("totalFileCount", totalFileCount);
      conf.set("fileTokenCountPath", fileTokenCountPath.toString());
      conf.set("inverseDocumentFreqPath", inverseDocumentFreqPath.toString());

      if (!fs.exists(tempPath1) && !runJob1(inputPath, tempPath1)) System.exit(1);
      if (!fs.exists(tempPath2) && !runJob2(tempPath1, tempPath2)) System.exit(1);
      if (!fs.exists(outputPath) && !runJob3(tempPath2, outputPath)) System.exit(1);
    }
    return 0;
  }

  private boolean runJob1(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
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
    FileOutputFormat.setOutputPath(job1, outputPath);

    return job1.waitForCompletion(true);
  }

  private boolean runJob2(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    var conf = getConf();
    var job2 = Job.getInstance(conf, "token count");
    job2.setJarByClass(TokenCount.class);

    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapperClass(TokenCount.Map.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(TokenPositionsWritable.class);

    var totalFileCount = conf.getLong("totalFileCount", 1);
    job2.setReducerClass(TokenCount.Reduce.class);
    job2.setNumReduceTasks((int) totalFileCount);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(TermFreqWritable.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, outputPath);

    return job2.waitForCompletion(true);
  }

  private boolean runJob3(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    var job3 = Job.getInstance(getConf(), "inverted index");
    job3.setJarByClass(InvertedIndex.class);

    job3.setInputFormatClass(SequenceFileInputFormat.class);
    job3.setMapperClass(InvertedIndex.Map.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(TermFreqWritable.class);

    job3.setReducerClass(InvertedIndex.Reduce.class);
    job3.setNumReduceTasks(NUM_REDUCE_TASKS);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(TermFreqWritable.class);

    FileInputFormat.addInputPath(job3, inputPath);
    FileOutputFormat.setOutputPath(job3, outputPath);

    return job3.waitForCompletion(true);
  }
}
