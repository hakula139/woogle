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

import java.io.*;
import java.util.HashMap;

public class Driver extends Configured implements Tool {
  public static final int NUM_REDUCE_TASKS = 128;
  public static final String FILE_TOKEN_COUNT_FILENAME = "file_token_count.txt";

  public static final HashMap<String, Long> fileTokenCount = new HashMap<>();
  public static long totalFileCount = 0;

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

    var conf = getConf();
    try (var fs = FileSystem.get(conf)) {
      totalFileCount = fs.getContentSummary(inputPath).getFileCount();
      if (totalFileCount == 0) return 0;

      if (!fs.exists(tempPath1) && !runJob1(inputPath, tempPath1)) System.exit(1);
      if (!fs.exists(tempPath2) && !runJob2(tempPath1, tempPath2, fs)) System.exit(1);
      if (!fs.exists(outputPath) && !runJob3(tempPath2, outputPath, fs)) System.exit(1);
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

  private boolean runJob2(Path inputPath, Path outputPath, FileSystem fs)
      throws IOException, InterruptedException, ClassNotFoundException {
    var job2 = Job.getInstance(getConf(), "token count");
    job2.setJarByClass(TokenCount.class);

    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapperClass(TokenCount.Map.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(TokenPositionsWritable.class);

    job2.setReducerClass(TokenCount.Reduce.class);
    job2.setNumReduceTasks((int) totalFileCount);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(TermFreqWritable.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, outputPath);

    var ret = job2.waitForCompletion(true);
    dumpToFile(new Path(outputPath.getParent(), FILE_TOKEN_COUNT_FILENAME), fs);
    return ret;
  }

  private boolean runJob3(Path inputPath, Path outputPath, FileSystem fs)
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
    job3.setOutputValueClass(InvertedIndexWritable.class);

    FileInputFormat.addInputPath(job3, inputPath);
    FileOutputFormat.setOutputPath(job3, outputPath);

    loadFromFile(new Path(inputPath.getParent(), FILE_TOKEN_COUNT_FILENAME), fs);
    return job3.waitForCompletion(true);
  }

  protected void dumpToFile(Path path, FileSystem fs) throws IOException {
    try (var writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)))) {
      for (var entry : fileTokenCount.entrySet()) {
        writer.write(entry.getKey() + "\t" + entry.getValue() + "\n");
      }
    }
  }

  protected void loadFromFile(Path path, FileSystem fs) throws IOException {
    try (var reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
      var line = "";
      while ((line = reader.readLine()) != null) {
        var lineSplit = line.split("\t");
        var filename = lineSplit[0];
        var totalTokenCount = Long.valueOf(lineSplit[1]);
        fileTokenCount.put(filename, totalTokenCount);
      }
    }
  }
}
