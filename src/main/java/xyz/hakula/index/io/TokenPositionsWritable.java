package xyz.hakula.index.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TokenPositionsWritable implements Writable {
  private static final String DELIM = ":";

  private Text token;
  private LongArrayWritable positions;

  public TokenPositionsWritable() {
    this.token = new Text();
    this.positions = new LongArrayWritable();
  }

  public TokenPositionsWritable(Text token, LongArrayWritable positions) {
    this.token = token;
    this.positions = positions;
  }

  public TokenPositionsWritable(String token, Writable[] positions) {
    this.token = new Text(token);
    this.positions = new LongArrayWritable(positions);
  }

  public void set(Text token, LongArrayWritable positions) {
    this.token = token;
    this.positions = positions;
  }

  public void set(String token, Writable[] positions) {
    this.token.set(token);
    this.positions.set(positions);
  }

  public String getToken() {
    return token.toString();
  }

  public Writable[] getPositions() {
    return positions.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    token.write(out);
    positions.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    token.readFields(in);
    positions.readFields(in);
  }

  @Override
  public String toString() {
    return token + DELIM + positions;
  }
}
