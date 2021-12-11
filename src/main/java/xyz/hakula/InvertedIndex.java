package xyz.hakula;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import xyz.hakula.io.InvertedIndexWritable;
import xyz.hakula.io.TermFreqWritable;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedIndex {
  public static class Map extends Mapper<Text, TermFreqWritable, Text, TermFreqWritable> {
    @Override
    public void map(Text key, TermFreqWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class Reduce extends Reducer<Text, TermFreqWritable, Text, InvertedIndexWritable> {
    private final InvertedIndexWritable value = new InvertedIndexWritable();

    // Combine the Term Frequencies (TFs) of each token,
    // and yield the Inverse Document Frequency (IDF).
    // (<token>, <filename>:<tokenCount>:<tf>:[<offsets>])
    // -> (<token>, <idf> [<filename>:<tokenCount>:<tf>:[<offsets>]])
    @Override
    public void reduce(Text key, Iterable<TermFreqWritable> values, Context context)
        throws IOException, InterruptedException {
      var conf = context.getConfiguration();

      var termFreqList = new ArrayList<TermFreqWritable>();
      long fileCount = 0;
      for (var value : values) {
        termFreqList.add(WritableUtils.clone(value, conf));
        ++fileCount;
      }

      var totalFileCount = conf.getLong("totalFileCount", 1);
      var inverseDocumentFreq = (double) fileCount / totalFileCount;
      this.value.set(inverseDocumentFreq, termFreqList.toArray(TermFreqWritable[]::new));
      context.write(key, this.value);
    }
  }
}
