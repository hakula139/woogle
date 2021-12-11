package xyz.hakula.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TermFreqWritable implements Writable {
  private static final String DELIM = ":";
  private static final String POS_ARRAY_DELIM = ";";

  private Text filename;
  private DoubleWritable termFreq;
  private LongArrayWritable positions;

  public TermFreqWritable() {
    this.filename = new Text();
    this.termFreq = new DoubleWritable();
    this.positions = new LongArrayWritable();
  }

  public TermFreqWritable(Text filename, DoubleWritable termFreq, LongArrayWritable positions) {
    this.filename = filename;
    this.termFreq = termFreq;
    this.positions = positions;
  }

  public TermFreqWritable(String filename, double termFreq, Writable[] positions) {
    this.filename = new Text(filename);
    this.termFreq = new DoubleWritable(termFreq);
    this.positions = new LongArrayWritable(positions);
  }

  public void set(Text filename, DoubleWritable termFreq, LongArrayWritable positions) {
    this.filename = filename;
    this.termFreq = termFreq;
    this.positions = positions;
  }

  public void set(String filename, double termFreq, LongArrayWritable positions) {
    this.filename.set(filename);
    this.termFreq.set(termFreq);
    this.positions = positions;
  }

  public void set(String filename, double termFreq, Writable[] positions) {
    this.filename.set(filename);
    this.termFreq.set(termFreq);
    this.positions.set(positions);
  }

  public String getFilename() {
    return filename.toString();
  }

  public double getTermFreq() {
    return termFreq.get();
  }

  public Writable[] getPositions() {
    return positions.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    filename.write(out);
    termFreq.write(out);
    positions.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    filename.readFields(in);
    termFreq.readFields(in);
    positions.readFields(in);
  }

  @Override
  public String toString() {
    return String.join(
        DELIM,
        filename.toString(),
        termFreq.toString(),
        String.join(POS_ARRAY_DELIM, positions.toStrings())
    );
  }
}
