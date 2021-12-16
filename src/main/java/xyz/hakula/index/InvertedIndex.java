package xyz.hakula.index;

import org.apache.hadoop.conf.Configuration;
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
      long fileTokenCount = readFromFile(context, value.getFilename());
      value.setTermFreq((double) value.getTokenCount() / fileTokenCount);
      context.write(key, value);
    }

    private long readFromFile(Context context, String key) throws IOException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      String fileTokenCountPath = conf.get("fileTokenCountPath");
      Path inputPath = new Path(fileTokenCountPath, key);
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
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
      for (TermFreqWritable value : values) {
        context.write(key, value);
        ++fileCount;
      }
      writeToFile(context, key.toString(), fileCount);
    }

    private void writeToFile(Context context, String key, long fileCount)
        throws IOException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      String inverseDocumentFreqPath = conf.get("inverseDocumentFreqPath");
      Path outputPath = new Path(inverseDocumentFreqPath, key.replaceAll("\\W+", "_"));

      long totalFileCount = conf.getLong("totalFileCount", 1);
      double inverseDocumentFreq = Math.log((double) totalFileCount / fileCount) / Math.log(2);
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(fs.create(outputPath, true))
      )) {
        writer.write(fileCount + " " + inverseDocumentFreq + "\n");
      }
    }
  }
}
