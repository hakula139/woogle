package xyz.hakula;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TokenCount {
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    }
  }

  public static class TokenReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      var fileIndex = new StringBuilder();
      long fileCount = 0;
      for (var value : values) {
        fileIndex.append(FILE_DELIM).append(value);
        ++fileCount;
      }
      var idf = Math.log((double) totalFileCount / fileCount);

      result.set(idf + fileIndex.toString());
      context.write(key, result);
    }
  }
}
