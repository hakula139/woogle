package xyz.hakula.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongArrayWritable extends ArrayWritable {
  public LongArrayWritable() {
    super(LongWritable.class);
  }

  public LongArrayWritable(Writable[] values) {
    super(LongWritable.class, values);
  }
}
