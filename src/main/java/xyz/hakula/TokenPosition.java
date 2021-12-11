package xyz.hakula;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Locale;
import java.util.StringTokenizer;

public class TokenPosition {
  private static final String TOKEN_DELIM = "@";
  private static final String POS_LIST_DELIM = ";";

  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text key = new Text();
    private final LongWritable offset = new LongWritable();

    // Yield the byte offset of a token in each file.
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      var filename = ((FileSplit) context.getInputSplit()).getPath().getName();
      var offset = key.get();  // byte offset

      var it = new StringTokenizer(value.toString(), " \t\r\f");
      while (it.hasMoreTokens()) {
        var token = it.nextToken().toLowerCase(Locale.ROOT);
        this.key.set(token + TOKEN_DELIM + filename);
        this.offset.set(offset);
        context.write(this.key, this.offset);

        // Suppose all words are separated with a single whitespace character.
        offset += token.getBytes(StandardCharsets.UTF_8).length + 1;
      }
    }
  }

  public static class Reduce extends Reducer<Text, LongWritable, Text, ArrayWritable> {
    private final ArrayWritable offsets = new ArrayWritable(LongWritable.class);

    // Yield all occurrences of a token in each file.
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      var offsets = new ArrayList<LongWritable>();
      for (var value : values) {
        offsets.add(WritableUtils.clone(value, context.getConfiguration()));
      }
      offsets.sort(LongWritable::compareTo);
      this.offsets.set(offsets.toArray(LongWritable[]::new));
      context.write(key, this.offsets);
    }
  }
}
