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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Woogle extends Configured implements Tool {
  private static final String INPUT_PATH = "output";
  private static final String RED = "\\e[0;31m";
  private static final String RESET = "\\e[0;0m";

  private static final Logger log = Logger.getLogger(Woogle.class.getName());

  public static void main(String[] args) throws Exception {
    var conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Woogle(), args));
  }

  public int run(String[] args) throws Exception {
    var scanner = new Scanner(System.in);
    System.out.print("Please input a keyword:\n> ");
    var key = scanner.nextLine().trim();
    var partition = getPartition(key);
    var inputPath = new Path(INPUT_PATH, String.format("part-r-%05d", partition));

    var conf = getConf();
    var fs = FileSystem.get(conf);
    try (var reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
      search(reader, key);
    }
    return 0;
  }

  protected int getPartition(String key) {
    var textKey = new Text(key);
    return (textKey.hashCode() & Integer.MAX_VALUE) % Driver.NUM_REDUCE_TASKS;
  }

  protected void search(BufferedReader reader, String key) throws IOException {
    SearchResult result = null;
    var line = "";
    while ((line = reader.readLine()) != null) {
      var lineSplit = line.split("\t");
      var token = lineSplit[0];
      if (!token.contains(key)) continue;

      try {
        result = SearchResult.parse(lineSplit[1]);
      } catch (Exception e) {
        log.warn(token + ": invalid index entry, error: " + e);
        continue;
      }

      token = token.replace(key, RED + key + RESET);
      printResult(token, result);
    }
    if (result == null) printResult(key, null);
  }

  protected void printResult(String token, SearchResult result) {
    if (result == null) {
      System.out.println(token + ": not found\n");
      return;
    }

    var idf = result.getInverseDocumentFreq();
    System.out.printf("%s: IDF = %6e | found in files:\n", token, idf);

    for (var termFreq : result.getTermFreqs()) {
      var filename = termFreq.getFilename();
      var tokenCount = termFreq.getTokenCount();
      var tf = termFreq.getTermFreq();
      System.out.printf(
          "  %s: TF = %6e (%d times) | TF-IDF = %6e | positions:",
          filename,
          tf,
          tokenCount,
          tf * idf
      );

      for (var position : termFreq.getPositions()) {
        System.out.print(" ");
        System.out.print(position);
      }
      System.out.println("\n");
    }
  }
}
