package xyz.hakula.woogle.model;

import java.util.regex.Pattern;

public class SearchResult {
  private static final String DELIM = " ";

  private final double inverseDocumentFreq;
  private final TermFreq[] termFreqs;

  public SearchResult(double inverseDocumentFreq, TermFreq[] termFreqs) {
    this.inverseDocumentFreq = inverseDocumentFreq;
    this.termFreqs = termFreqs;
  }

  public double inverseDocumentFreq() {
    return inverseDocumentFreq;
  }

  public TermFreq[] termFreqs() {
    return termFreqs;
  }

  public static SearchResult parse(String entry) {
    String[] entrySplit = entry.split(Pattern.quote(DELIM));
    double inverseDocumentFreq = Double.parseDouble(entrySplit[0]);
    TermFreq[] termFreqs = TermFreq.parseArray(entrySplit[1]);
    return new SearchResult(inverseDocumentFreq, termFreqs);
  }
}
