package xyz.hakula.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.index.io.LongArrayWritable;
import xyz.hakula.index.io.TermFreqWritable;
import xyz.hakula.index.io.TokenFromFileWritable;
import xyz.hakula.index.io.TokenPositionsWritable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class TokenCount {
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

    // Yield the token count of each token in each file,
    // and calculate the total token count of each file.
    // (<filename>, (<token>, [<offset>]))
    // -> (<token>, (<filename>, <token_count>, 0, [<positions>]))
    @Override
    public void reduce(Text key, Iterable<TokenPositionsWritable> values, Context context)
        throws IOException, InterruptedException {
      var filename = key.toString();
      long totalTokenCount = 0;
      for (var value : values) {
        var positions = value.getPositions();
        var tokenCount = positions.length;
        this.key.set(value.getToken());
        // The Term Frequency (TF) will be calculated in next job, and hence left blank here.
        this.value.set(filename, tokenCount, 0, positions);
        context.write(this.key, this.value);
        totalTokenCount += tokenCount;
      }
      writeToFile(context, key.toString(), totalTokenCount);
    }

    private void writeToFile(Context context, String key, long totalTokenCount) throws IOException {
      var conf = context.getConfiguration();
      var fs = FileSystem.get(conf);
      var fileTokenCountPath = conf.get("fileTokenCountPath");
      var outputPath = new Path(fileTokenCountPath, key);
      try (var writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))) {
        writer.write(totalTokenCount + "\n");
      }
    }
  }
}
