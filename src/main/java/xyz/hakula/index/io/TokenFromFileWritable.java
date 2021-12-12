package xyz.hakula.index.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TokenFromFileWritable implements WritableComparable<TokenFromFileWritable> {
  private static final String DELIM = "@";

  private Text token;
  private Text filename;

  public TokenFromFileWritable() {
    this.token = new Text();
    this.filename = new Text();
  }

  public TokenFromFileWritable(Text token, Text filename) {
    this.token = token;
    this.filename = filename;
  }

  public TokenFromFileWritable(String token, String filename) {
    this.token = new Text(token);
    this.filename = new Text(filename);
  }

  public void set(Text token, Text filename) {
    this.token = token;
    this.filename = filename;
  }

  public void set(String token, String filename) {
    this.token.set(token);
    this.filename.set(filename);
  }

  public String getToken() {
    return token.toString();
  }

  public String getFilename() {
    return filename.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    token.write(out);
    filename.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    token.readFields(in);
    filename.readFields(in);
  }

  @Override
  public int hashCode() {
    return token.hashCode() ^ filename.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TokenFromFileWritable) {
      return compareTo((TokenFromFileWritable) o) == 0;
    }
    return false;
  }

  @Override
  public String toString() {
    return token + DELIM + filename;
  }

  @Override
  public int compareTo(TokenFromFileWritable o) {
    int tokenRelation = token.compareTo(o.token);
    if (tokenRelation != 0) return tokenRelation;
    return filename.compareTo(o.filename);
  }
}
