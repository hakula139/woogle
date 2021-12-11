package xyz.hakula.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TokenPositionArrayWritable implements Writable {
  private static final String DELIM = ":";
  private static final String POS_ARRAY_DELIM = ";";

  private Text token;
  private ArrayWritable positions;

  public TokenPositionArrayWritable() {
    this.token = new Text();
    this.positions = new ArrayWritable(Writable.class);
  }

  public TokenPositionArrayWritable(Text token, ArrayWritable positions) {
    this.token = token;
    this.positions = positions;
  }

  public TokenPositionArrayWritable(String token, Writable[] positions) {
    this.token = new Text(token);
    this.positions = new ArrayWritable(Writable.class, positions);
  }

  public void set(Text token, ArrayWritable positions) {
    this.token = token;
    this.positions = positions;
  }

  public void set(String token, ArrayWritable positions) {
    this.token.set(token);
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
  public void readFields(DataInput in) throws IOException {
    token.readFields(in);
    positions.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    token.write(out);
    positions.write(out);
  }

  @Override
  public String toString() {
    return token.toString() + DELIM + String.join(POS_ARRAY_DELIM, positions.toStrings());
  }
}
