package xyz.hakula.index.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InverseDocumentFreqWritable implements Writable {
  private static final String PREFIX = "$";
  private static final String DELIM = ":";

  private LongWritable fileCount;
  private DoubleWritable inverseDocumentFreq;

  public InverseDocumentFreqWritable() {
    this.fileCount = new LongWritable();
    this.inverseDocumentFreq = new DoubleWritable();
  }

  public InverseDocumentFreqWritable(LongWritable fileCount, DoubleWritable inverseDocumentFreq) {
    this.fileCount = fileCount;
    this.inverseDocumentFreq = inverseDocumentFreq;
  }

  public InverseDocumentFreqWritable(long fileCount, double inverseDocumentFreq) {
    this.fileCount = new LongWritable(fileCount);
    this.inverseDocumentFreq = new DoubleWritable(inverseDocumentFreq);
  }

  public void set(LongWritable fileCount, DoubleWritable inverseDocumentFreq) {
    this.fileCount = fileCount;
    this.inverseDocumentFreq = inverseDocumentFreq;
  }

  public void set(long fileCount, double inverseDocumentFreq) {
    this.fileCount.set(fileCount);
    this.inverseDocumentFreq.set(inverseDocumentFreq);
  }

  public long getFileCount() {
    return fileCount.get();
  }

  public double getInverseDocumentFreq() {
    return inverseDocumentFreq.get();
  }

  public void setInverseDocumentFreq(double inverseDocumentFreq) {
    this.inverseDocumentFreq.set(inverseDocumentFreq);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    fileCount.write(out);
    inverseDocumentFreq.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fileCount.readFields(in);
    inverseDocumentFreq.readFields(in);
  }

  @Override
  public String toString() {
    return PREFIX + fileCount + DELIM + inverseDocumentFreq;
  }
}
