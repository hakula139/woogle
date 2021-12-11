package xyz.hakula.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class TermFreqArrayWritable extends ArrayWritable {
  public TermFreqArrayWritable() {
    super(TermFreqWritable.class);
  }

  public TermFreqArrayWritable(Writable[] values) {
    super(TermFreqWritable.class, values);
  }
}
