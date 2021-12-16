package xyz.hakula.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.index.io.TermFreqWritable;

import java.io.*;

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

  public static class Reduce extends Reducer<Text, TermFreqWritable, Text, TermFreqWritable> {
    // Yield the Inverse Document Frequency (IDF) of each token.
    @Override
    public void reduce(Text key, Iterable<TermFreqWritable> values, Context context)
        throws IOException, InterruptedException {
      long fileCount = 0;
      for (var value : values) {
        context.write(key, value);
        ++fileCount;
      }
      writeToFile(context, key.toString(), fileCount);
    }

    private void writeToFile(Context context, String key, long fileCount)
        throws IOException {
      var conf = context.getConfiguration();
      var fs = FileSystem.get(conf);
      var inverseDocumentFreqPath = conf.get("inverseDocumentFreqPath");
      var outputPath = new Path(inverseDocumentFreqPath, key.replaceAll("\\W+", "_"));

      var totalFileCount = conf.getLong("totalFileCount", 1);
      var inverseDocumentFreq = Math.log((double) totalFileCount / fileCount) / Math.log(2);
      try (var writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))) {
        writer.write(fileCount + " " + inverseDocumentFreq + "\n");
      }
    }
  }
}
