package xyz.hakula;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.io.LongArrayWritable;
import xyz.hakula.io.TermFreqWritable;
import xyz.hakula.io.TokenFromFileWritable;
import xyz.hakula.io.TokenPositionsWritable;

import java.io.IOException;
import java.util.ArrayList;

public class TermFreq {
  public static class Map
      extends Mapper<TokenFromFileWritable, LongArrayWritable, Text, TokenPositionsWritable> {
    private final Text key = new Text();
    private final TokenPositionsWritable value = new TokenPositionsWritable();

    // (<token>@<filename>, [<offset>]) -> (<filename>, (<token>, [<offset>]))
    @Override
    public void map(TokenFromFileWritable key, LongArrayWritable value, Context context)
        throws IOException, InterruptedException {
      this.key.set(key.getFilename());
      this.value.set(key.getToken(), value);
      context.write(this.key, this.value);
    }
  }

  public static class Combine
      extends Reducer<Text, TokenPositionsWritable, Text, TermFreqWritable> {
    private final Text key = new Text();
    private final TermFreqWritable value = new TermFreqWritable();

    // Yield the Term Frequency (TF) of each token in each file.
    // (<filename>, (<token>, [<offset>])) -> (<token>, <filename>:<tf>:[<offsets>])
    @Override
    public void reduce(Text key, Iterable<TokenPositionsWritable> values, Context context)
        throws IOException, InterruptedException {
      var tokenPositionsList = new ArrayList<TokenPositionsWritable>();
      long tokenCount = 0;
      for (var value : values) {
        tokenPositionsList.add(value);
        tokenCount += value.getPositions().length;
      }

      var filename = key.toString();
      for (var tokenPositions : tokenPositionsList) {
        var token = tokenPositions.getToken();
        var positions = tokenPositions.getPositions();
        var termFreq = positions.length / tokenCount;

        this.key.set(token);
        this.value.set(filename, termFreq, positions);
        context.write(this.key, this.value);
      }
    }
  }
}
