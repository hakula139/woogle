package xyz.hakula;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.io.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TermFreq {
  public static class Map
      extends Mapper<TokenFromFileWritable, LongArrayWritable, Text, TokenPositionArrayWritable> {
    private final Text key = new Text();
    private final TokenPositionArrayWritable value = new TokenPositionArrayWritable();

    // (<token>@<filename>, <offsets>) -> (<filename>, (<token>, <offsets>))
    @Override
    public void map(TokenFromFileWritable key, LongArrayWritable value, Context context)
        throws IOException, InterruptedException {
      this.key.set(key.getFilename());
      this.value.set(key.getToken(), value);
      context.write(this.key, this.value);
    }
  }

  public static class Reduce
      extends Reducer<Text, TokenPositionArrayWritable, Text, TermFreqArrayWritable> {
    private final Text key = new Text();
    private final TermFreqArrayWritable value = new TermFreqArrayWritable();

    // Yield the Term Frequency (TF) of each token.
    @Override
    public void reduce(Text key, Iterable<TokenPositionArrayWritable> values, Context context)
        throws IOException, InterruptedException {
      var filename = key.toString();
      var tokenTfsMap = new HashMap<String, ArrayList<TermFreqWritable>>();
      long tokenCount = 0;

      for (var value : values) {
        var token = value.getToken();
        var positions = value.getPositions();
        var tf = new TermFreqWritable(filename, positions.length, value.getPositions());
        if (tokenTfsMap.containsKey(token)) {
          tokenTfsMap.get(token).add(tf);
        } else {
          tokenTfsMap.put(token, new ArrayList<>(List.of(tf)));
        }
        tokenCount += value.getPositions().length;
      }

      final long finalTokenCount = tokenCount;
      for (var tokenTfs : tokenTfsMap.entrySet()) {
        var token = tokenTfs.getKey();
        var tfs = tokenTfs.getValue();

        // Update the TFs to their real values.
        for (var tf : tfs) {
          tf.setTermFreq(tf.getTermFreq() / finalTokenCount);
        }

        this.key.set(token);
        this.value.set(tfs.toArray(TermFreqWritable[]::new));
        context.write(this.key, this.value);
      }
    }
  }
}
