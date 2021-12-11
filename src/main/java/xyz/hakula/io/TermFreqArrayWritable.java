package xyz.hakula.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class TermFreqArrayWritable extends ArrayWritable {
  private static final String DELIM = "|";

  public TermFreqArrayWritable() {
    super(TermFreqWritable.class);
  }

  public TermFreqArrayWritable(Writable[] values) {
    super(TermFreqWritable.class, values);
  }

  @Override
  public String toString() {
    return String.join(DELIM, toStrings());
  }
}
