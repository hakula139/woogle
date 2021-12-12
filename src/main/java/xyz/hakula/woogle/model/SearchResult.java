package xyz.hakula.woogle.model;

import java.util.regex.Pattern;

public record SearchResult(double inverseDocumentFreq, TermFreq[] termFreqs) {
  private static final String DELIM = " ";

  public static SearchResult parse(String entry) {
    var entrySplit = entry.split(Pattern.quote(DELIM));
    var inverseDocumentFreq = Double.parseDouble(entrySplit[0]);
    var termFreqs = TermFreq.parseArray(entrySplit[1]);
    return new SearchResult(inverseDocumentFreq, termFreqs);
  }
}
