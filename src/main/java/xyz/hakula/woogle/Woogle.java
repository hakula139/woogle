package xyz.hakula.woogle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import xyz.hakula.index.Driver;
import xyz.hakula.woogle.model.SearchResult;
import xyz.hakula.woogle.model.TermFreq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.Scanner;

public class Woogle extends Configured implements Tool {
  private static final String INPUT_PATH = "output";
  private static final String ANSI_RED = "\033[1;31m";
  private static final String ANSI_RESET = "\033[0m";

  private static final Logger log = Logger.getLogger(Woogle.class.getName());

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Woogle(), args));
  }

  public int run(String[] args) throws Exception {
    Scanner scanner = new Scanner(System.in);
    System.out.print("Please input a keyword:\n> ");
    String key = scanner.nextLine().trim().toLowerCase(Locale.ROOT);
    int partition = getPartition(key);
    Path inputPath = new Path(INPUT_PATH, String.format("part-r-%05d", partition));

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
      search(reader, key);
    }
    return 0;
  }

  protected int getPartition(String key) {
    Text textKey = new Text(key);
    return (textKey.hashCode() & Integer.MAX_VALUE) % Driver.NUM_REDUCE_TASKS;
  }

  protected void search(BufferedReader reader, String key) throws IOException {
    SearchResult result = null;
    String line;
    while ((line = reader.readLine()) != null) {
      String[] lineSplit = line.split("\t");
      String token = lineSplit[0];
      if (!token.contains(key)) continue;

      try {
        result = SearchResult.parse(lineSplit[1]);
      } catch (Exception e) {
        log.warn(token + ": invalid index entry, error: " + e);
        continue;
      }

      token = token.replace(key, ANSI_RED + key + ANSI_RESET);
      printResult(token, result);
    }
    if (result == null) printResult(key, null);
  }

  protected void printResult(String token, SearchResult result) {
    if (result == null) {
      System.out.println(token + ": not found");
      return;
    }

    double idf = result.inverseDocumentFreq();
    System.out.printf("%s: IDF = %6e | found in files:\n", token, idf);

    for (TermFreq termFreq : result.termFreqs()) {
      String filename = termFreq.filename();
      long tokenCount = termFreq.tokenCount();
      double tf = termFreq.termFreq();
      System.out.printf(
          "  %s: TF = %6e (%d times) | TF-IDF = %6e | positions:",
          filename,
          tf,
          tokenCount,
          tf * idf
      );

      for (long position : termFreq.positions()) {
        System.out.print(" ");
        System.out.print(position);
      }
      System.out.println();
    }
  }
}
