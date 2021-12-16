package xyz.hakula.woogle.model;

import java.util.regex.Pattern;

public record InverseDocumentFreq(long fileCount, double inverseDocumentFreq) {
  private static final String PREFIX = "$";
  private static final String DELIM = ":";

  public static InverseDocumentFreq parse(String entry) {
    var entrySplit = entry.substring(PREFIX.length()).split(Pattern.quote(DELIM));
    var fileCount = Long.parseLong(entrySplit[0]);
    var inverseDocumentFreq = Double.parseDouble(entrySplit[1]);
    return new InverseDocumentFreq(fileCount, inverseDocumentFreq);
  }
}
