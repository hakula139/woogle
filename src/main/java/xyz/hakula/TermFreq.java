package xyz.hakula;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.io.TermFreqWritable;
import xyz.hakula.io.TokenFromFileWritable;
import xyz.hakula.io.TokenPositionArrayWritable;

import java.io.IOException;

public class TermFreq {
  public static class Map
      extends Mapper<TokenFromFileWritable, ArrayWritable, Text, TokenPositionArrayWritable> {
    private final Text key = new Text();
    private final TokenPositionArrayWritable value = new TokenPositionArrayWritable();

    // (<token>@<filename>, <offsets>) -> (<filename>, (<token>, <offsets>))
    @Override
    public void map(TokenFromFileWritable key, ArrayWritable value, Context context)
        throws IOException, InterruptedException {
      this.key.set(key.getFilename());
      this.value.set(key.getToken(), value);
      context.write(this.key, this.value);
    }
  }

  public static class Reduce
      extends Reducer<Text, TokenPositionArrayWritable, Text, TermFreqWritable> {
    private final Text key = new Text();
    private final TermFreqWritable value = new TermFreqWritable();

    // Yield the Term Frequency (TF) of each token.
    @Override
    public void reduce(Text key, Iterable<TokenPositionArrayWritable> values, Context context)
        throws IOException, InterruptedException {
      var tokenCount = 0;
      for (var value : values) {
        tokenCount += value.getPositions().length;
      }

      var filename = key.toString();
      for (var value : values) {
        var termFreq = (double) value.getPositions().length / tokenCount;
        this.key.set(value.getToken());
        this.value.set(filename, termFreq, value.getPositions());
        context.write(this.key, this.value);
      }
    }
  }
}
