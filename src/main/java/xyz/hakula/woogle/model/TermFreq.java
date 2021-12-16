package xyz.hakula.woogle.model;

import java.util.Arrays;
import java.util.regex.Pattern;

public class TermFreq {
  private static final String DELIM = ":";
  private static final String POS_ARRAY_DELIM = ";";

  private final String filename;
  private final long tokenCount;
  private final double termFreq;
  private final long[] positions;

  public TermFreq(String filename, long tokenCount, double termFreq, long[] positions) {
    this.filename = filename;
    this.tokenCount = tokenCount;
    this.termFreq = termFreq;
    this.positions = positions;
  }

  public static TermFreq parse(String entry) {
    String[] entrySplit = entry.split(Pattern.quote(DELIM));
    String filename = entrySplit[0];
    long tokenCount = Long.parseLong(entrySplit[1]);
    double termFreq = Double.parseDouble(entrySplit[2]);
    String[] positionsSplit = entrySplit[3].split(Pattern.quote(POS_ARRAY_DELIM));
    long[] positions = Arrays.stream(positionsSplit).mapToLong(Long::parseLong).toArray();
    return new TermFreq(filename, tokenCount, termFreq, positions);
  }

  public String filename() {
    return filename;
  }

  public long tokenCount() {
    return tokenCount;
  }

  public double termFreq() {
    return termFreq;
  }

  public long[] positions() {
    return positions;
  }
}
