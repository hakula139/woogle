package xyz.hakula.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.index.io.TermFreqWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class InvertedIndex {
  public static class Map extends Mapper<Text, TermFreqWritable, Text, TermFreqWritable> {
    // Yield the Term Frequency (TF) of each token in each file.
    @Override
    public void map(Text key, TermFreqWritable value, Context context)
        throws IOException, InterruptedException {
      var fileTokenCount = readFromFile(context, value.getFilename());
      value.setTermFreq((double) value.getTokenCount() / fileTokenCount);
      context.write(key, value);
    }

    private long readFromFile(Context context, String key) throws IOException {
      var conf = context.getConfiguration();
      var fs = FileSystem.get(conf);
      var fileTokenCountPath = conf.get("fileTokenCountPath");
      var inputPath = new Path(fileTokenCountPath, key);
      try (var reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
        return Long.parseLong(reader.readLine());
      }
    }
  }

  public static class Reduce extends Reducer<Text, TermFreqWritable, Text, Text> {
    private static final String IDF_SIGN = "$";
    private static final String DELIM = " ";

    private final Text value = new Text();

    // Yield the Inverse Document Frequency (IDF) of each token.
    @Override
    public void reduce(Text key, Iterable<TermFreqWritable> values, Context context)
        throws IOException, InterruptedException {
      long fileCount = 0;
      for (var value : values) {
        this.value.set(value.toString());
        context.write(key, this.value);
        ++fileCount;
      }

      var conf = context.getConfiguration();
      var totalFileCount = conf.getLong("totalFileCount", 1);
      var inverseDocumentFreq = Math.log((double) totalFileCount / fileCount) / Math.log(2);
      this.value.set(IDF_SIGN + fileCount + DELIM + inverseDocumentFreq);
      context.write(key, this.value);
    }
  }
}
