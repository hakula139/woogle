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
import java.util.Map.Entry;

public class Driver extends Configured implements Tool {
  public static final int NUM_REDUCE_TASKS = 128;

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Driver(), args));
  }

  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Path tempPath = new Path(args[2]);
    Path tempPath1 = new Path(tempPath, "output_job1");
    Path tempPath2 = new Path(tempPath, "output_job2");
    Path fileTokenCountPath = new Path(tempPath, "file_token_count.txt");

    Configuration conf = getConf();
    try (FileSystem fs = FileSystem.get(conf)) {
      long totalFileCount = fs.getContentSummary(inputPath).getFileCount();
      if (totalFileCount == 0) return 0;
      conf.setLong("totalFileCount", totalFileCount);

      if (!fs.exists(tempPath1) && !runJob1(inputPath, tempPath1)) {
        System.exit(1);
      }
      if (!fs.exists(tempPath2) && !runJob2(tempPath1, tempPath2, fileTokenCountPath)) {
        System.exit(1);
      }
      if (!fs.exists(outputPath) && !runJob3(tempPath2, outputPath, fileTokenCountPath)) {
        System.exit(1);
      }
    }
    return 0;
  }

  private boolean runJob1(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job1 = Job.getInstance(getConf(), "token position");
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

  private boolean runJob2(Path inputPath, Path outputPath, Path fileTokenCountPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    Job job2 = Job.getInstance(conf, "token count");
    job2.setJarByClass(TokenCount.class);

    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapperClass(TokenCount.Map.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(TokenPositionsWritable.class);

    long totalFileCount = conf.getLong("totalFileCount", 1);
    job2.setReducerClass(TokenCount.Reduce.class);
    job2.setNumReduceTasks((int) totalFileCount);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(TermFreqWritable.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, outputPath);

    boolean ret = job2.waitForCompletion(true);
    dumpToFile(fileTokenCountPath);
    return ret;
  }

  private boolean runJob3(Path inputPath, Path outputPath, Path fileTokenCountPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job3 = Job.getInstance(getConf(), "inverted index");
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

    loadFromFile(fileTokenCountPath);
    return job3.waitForCompletion(true);
  }

  protected void dumpToFile(Path path) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(fs.create(path, true))
    )) {
      for (Entry<String, Long> entry : TokenCount.Reduce.fileTokenCount.entrySet()) {
        writer.write(entry.getKey() + "\t" + entry.getValue() + "\n");
      }
    }
  }

  protected void loadFromFile(Path path) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] lineSplit = line.split("\t");
        String filename = lineSplit[0];
        long totalTokenCount = Long.parseLong(lineSplit[1]);
        InvertedIndex.Map.fileTokenCount.put(filename, totalTokenCount);
      }
    }
  }
}
