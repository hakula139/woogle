package xyz.hakula.woogle.model;

import java.util.Arrays;
import java.util.regex.Pattern;

public record TermFreq(String filename, long tokenCount, double termFreq, long[] positions) {
  private static final String DELIM = ":";
  private static final String POS_ARRAY_DELIM = ";";

  public static TermFreq parse(String entry) {
    var entrySplit = entry.split(Pattern.quote(DELIM));
    var filename = entrySplit[0];
    var tokenCount = Long.parseLong(entrySplit[1]);
    var termFreq = Double.parseDouble(entrySplit[2]);
    var positionsSplit = entrySplit[3].split(Pattern.quote(POS_ARRAY_DELIM));
    var positions = Arrays.stream(positionsSplit).mapToLong(Long::parseLong).toArray();
    return new TermFreq(filename, tokenCount, termFreq, positions);
  }
}
