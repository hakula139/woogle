package xyz.hakula.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InvertedIndexWritable implements Writable {
  private static final String DELIM = " ";

  private DoubleWritable inverseDocumentFreq;
  private TermFreqArrayWritable termFreqs;

  public InvertedIndexWritable() {
    this.inverseDocumentFreq = new DoubleWritable();
    this.termFreqs = new TermFreqArrayWritable();
  }

  public InvertedIndexWritable(
      DoubleWritable inverseDocumentFreq, TermFreqArrayWritable termFreqs
  ) {
    this.inverseDocumentFreq = inverseDocumentFreq;
    this.termFreqs = termFreqs;
  }

  public InvertedIndexWritable(double inverseDocumentFreq, Writable[] termFreqs) {
    this.inverseDocumentFreq = new DoubleWritable(inverseDocumentFreq);
    this.termFreqs = new TermFreqArrayWritable(termFreqs);
  }

  public void set(DoubleWritable inverseDocumentFreq, TermFreqArrayWritable termFreqs) {
    this.inverseDocumentFreq = inverseDocumentFreq;
    this.termFreqs = termFreqs;
  }

  public void set(double inverseDocumentFreq, Writable[] termFreqs) {
    this.inverseDocumentFreq.set(inverseDocumentFreq);
    this.termFreqs.set(termFreqs);
  }

  public double getInverseDocumentFreq() {
    return inverseDocumentFreq.get();
  }

  public void setInverseDocumentFreq(double inverseDocumentFreq) {
    this.inverseDocumentFreq.set(inverseDocumentFreq);
  }

  public Writable[] getTermFreqs() {
    return termFreqs.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    inverseDocumentFreq.write(out);
    termFreqs.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inverseDocumentFreq.readFields(in);
    termFreqs.readFields(in);
  }

  @Override
  public String toString() {
    return String.format("%6e", inverseDocumentFreq.get()) + DELIM + termFreqs;
  }
}
