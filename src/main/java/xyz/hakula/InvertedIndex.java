package xyz.hakula;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;

public class InvertedIndex extends Configured implements Tool {
  private static final int NUM_REDUCE_TASKS = 16;
  private static final String GLOBAL_SIGN = "$_";
  private static final String DELIM = "::";
  private static final String POS_DELIM = ";";
  private static long inputFileCount;

  public static void main(String[] args) {
    try {
      var config = new Configuration();
      var fs = FileSystem.get(config);

      var inputPath = new Path(args[0]);
      inputFileCount = fs.getContentSummary(inputPath).getFileCount();

      var outputPath = new Path(args[1]);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }

      System.exit(ToolRunner.run(config, new InvertedIndex(), args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int run(String[] args) throws Exception {
    var job = Job.getInstance(getConf(), InvertedIndex.class.getName());
    job.setJarByClass(getClass());

    job.setMapperClass(TokenMapper.class);
    job.setCombinerClass(TokenCountCombiner.class);
    job.setPartitionerClass(Multiplexer.class);
    job.setNumReduceTasks(NUM_REDUCE_TASKS);
    job.setReducerClass(TokenCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class TokenMapper extends Mapper<Object, Text, Text, Text> {
    private final Text mapKey = new Text();
    private final Text position = new Text();
    private final Text count = new Text("1");

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      var filename = ((FileSplit) context.getInputSplit()).getPath().getName();
      var lines = value.toString().split("\n");

      for (var row = 0; row < lines.length; ++row) {
        var it = new StringTokenizer(lines[row], " \t\r\f");
        var col = 0;
        while (it.hasMoreTokens()) {
          var token = it.nextToken().toLowerCase(Locale.ROOT);
          // Suppose all words are separated with a single whitespace character.
          col += token.length() + 1;

          // Yield word positions in each file.
          mapKey.set(token + DELIM + filename);
          position.set((row + 1) + "," + (col + 1));
          context.write(mapKey, position);

          // Yield word count in each file.
          mapKey.set(GLOBAL_SIGN + filename);
          context.write(mapKey, count);
        }
      }
    }
  }

  public static class Multiplexer extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions)
        throws IndexOutOfBoundsException {
      var partitionKey = key.toString();
      if (partitionKey.startsWith(GLOBAL_SIGN)) {
        return 0;
      } else {
        return partitionKey.charAt(0) % (numPartitions - 1) + 1;
      }
    }
  }

  public static class TokenCountCombiner extends Reducer<Text, Text, Text, Text> {
    private final Text value = new Text();

    public void combineTokenCounts(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      value.set(String.valueOf(values.spliterator().getExactSizeIfKnown()));
      context.write(key, value);
    }

    public void combinePositions(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      var items = key.toString().split(DELIM);
      var token = items[0];
      var filename = items[1];

      var positions = new StringBuilder(filename);
      var count = values.spliterator().getExactSizeIfKnown();
      positions.append(DELIM).append(count).append(DELIM);
      for (var value : values) {
        positions.append(value).append(POS_DELIM);
      }

      key.set(token);
      value.set(positions.toString());
      context.write(key, value);
    }


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      if (key.toString().startsWith(GLOBAL_SIGN)) {
        combineTokenCounts(key, values, context);
      } else {
        combinePositions(key, values, context);
      }
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
}
