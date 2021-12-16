package xyz.hakula.woogle.model;

import java.util.regex.Pattern;

public class InverseDocumentFreq {
  private static final String DELIM = " ";

  private final long fileCount;
  private final double inverseDocumentFreq;

  public InverseDocumentFreq(long fileCount, double inverseDocumentFreq) {
    this.fileCount = fileCount;
    this.inverseDocumentFreq = inverseDocumentFreq;
  }

  public static InverseDocumentFreq parse(String entry) {
    String[] entrySplit = entry.split(Pattern.quote(DELIM));
    long fileCount = Long.parseLong(entrySplit[0]);
    double inverseDocumentFreq = Double.parseDouble(entrySplit[1]);
    return new InverseDocumentFreq(fileCount, inverseDocumentFreq);
  }

  public long fileCount() {
    return fileCount;
  }

  public double inverseDocumentFreq() {
    return inverseDocumentFreq;
  }
}
