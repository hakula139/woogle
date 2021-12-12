package xyz.hakula.woogle.model;

import java.util.Arrays;
import java.util.regex.Pattern;

public class TermFreq {
  private static final String DELIM = ":";
  private static final String ARRAY_DELIM = "|";
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

  public String getFilename() {
    return filename;
  }

  public long getTokenCount() {
    return tokenCount;
  }

  public double getTermFreq() {
    return termFreq;
  }

  public long[] getPositions() {
    return positions;
  }

  public static TermFreq parse(String entry) {
    var entrySplit = entry.split(Pattern.quote(DELIM));
    var filename = entrySplit[0];
    var tokenCount = Long.parseLong(entrySplit[1]);
    var termFreq = Double.parseDouble(entrySplit[2]);
    var positionsSplit = entrySplit[3].split(Pattern.quote(POS_ARRAY_DELIM));
    var positions = Arrays.stream(positionsSplit).mapToLong(Long::parseLong).toArray();
    return new TermFreq(filename, tokenCount, termFreq, positions);
  }

  public static TermFreq[] parseArray(String entries) {
    var entriesSplit = entries.split(Pattern.quote(ARRAY_DELIM));
    return Arrays.stream(entriesSplit).map(TermFreq::parse).toArray(TermFreq[]::new);
  }
}
