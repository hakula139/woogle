package xyz.hakula.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.index.io.InvertedIndexWritable;
import xyz.hakula.index.io.TermFreqWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

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

  public static class Reduce extends Reducer<Text, TermFreqWritable, Text, InvertedIndexWritable> {
    private final InvertedIndexWritable value = new InvertedIndexWritable();

    // Combine the Term Frequencies (TFs) of each token,
    // and yield the Inverse Document Frequency (IDF).
    // (<token>, (<filename>, <tokenCount>, 0, [<offsets>]))
    // -> (<token>, (<idf>, [(<token>, (<filename>, <tokenCount>, <tf>, [<offsets>]))]))
    @Override
    public void reduce(Text key, Iterable<TermFreqWritable> values, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      ArrayList<TermFreqWritable> termFreqList = new ArrayList<>();
      long fileCount = 0;
      for (TermFreqWritable value : values) {
        termFreqList.add(WritableUtils.clone(value, conf));
        ++fileCount;
      }

      long totalFileCount = conf.getLong("totalFileCount", 1);
      double idf = Math.log((double) totalFileCount / fileCount) / Math.log(2);
      this.value.set(idf, termFreqList.toArray(new TermFreqWritable[0]));
      context.write(key, this.value);
    }
  }
}
