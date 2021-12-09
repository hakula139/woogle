package xyz.hakula;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;

public class TokenPosition {
  private static final String TOKEN_DELIM = "@";
  private static final String POS_DELIM = ",";
  private static final String POS_LIST_DELIM = ";";

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key = new Text();
    private final Text position = new Text();

    // Yield a position of a token in each file.
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      var filename = ((FileSplit) context.getInputSplit()).getPath().getName();
      var row = key.get() + 1;
      var col = 1;

      var it = new StringTokenizer(value.toString(), " \t\r\f");
      while (it.hasMoreTokens()) {
        var token = it.nextToken().toLowerCase(Locale.ROOT);
        // Suppose all words are separated with a single whitespace character.
        col += token.length() + 1;

        this.key.set(token + TOKEN_DELIM + filename);
        position.set(row + POS_DELIM + col);
        context.write(this.key, position);
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private final Text value = new Text();

    // Yield all positions of a token in each file and its number of occurrence.
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      var items = key.toString().split(TOKEN_DELIM);
      var token = items[0];

      var positions = new StringBuilder();
      long tokenCount = 0;
      for (var value : values) {
        positions.append(POS_LIST_DELIM).append(value);
        ++tokenCount;
      }

      value.set(tokenCount + positions.toString());
      context.write(key, value);
    }
  }
}
