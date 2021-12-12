package xyz.hakula.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.index.io.LongArrayWritable;
import xyz.hakula.index.io.TermFreqWritable;
import xyz.hakula.index.io.TokenFromFileWritable;
import xyz.hakula.index.io.TokenPositionsWritable;

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
      this.value.set(key.getToken(), (Writable[]) value.toArray());
      context.write(this.key, this.value);
    }
  }

  public static class Reduce extends Reducer<Text, TokenPositionsWritable, Text, TermFreqWritable> {
    private final Text key = new Text();
    private final TermFreqWritable value = new TermFreqWritable();

    // Yield the Term Frequency (TF) of each token in each file.
    // (<filename>, (<token>, [<offset>]))
    // -> (<token>, <filename>:<tokenCount>:<tf>:[<offsets>])
    @Override
    public void reduce(Text key, Iterable<TokenPositionsWritable> values, Context context)
        throws IOException, InterruptedException {
      ArrayList<TokenPositionsWritable> tokenPositionsList = new ArrayList<>();
      long totalTokenCount = 0;
      for (TokenPositionsWritable value : values) {
        tokenPositionsList.add(WritableUtils.clone(value, context.getConfiguration()));
        totalTokenCount += value.getPositions().length;
      }

      String filename = key.toString();
      for (TokenPositionsWritable tokenPositions : tokenPositionsList) {
        String token = tokenPositions.getToken();
        Writable[] positions = tokenPositions.getPositions();
        long tokenCount = positions.length;
        double termFreq = (double) tokenCount / totalTokenCount;
        this.key.set(token);
        this.value.set(filename, tokenCount, termFreq, positions);
        context.write(this.key, this.value);
      }
    }
  }
}
