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
import xyz.hakula.index.InvertedIndex;
import xyz.hakula.woogle.model.InverseDocumentFreq;
import xyz.hakula.woogle.model.TermFreq;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.Objects;
import java.util.Scanner;

public class Woogle extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(Woogle.class.getName());

  public static void main(String[] args) throws Exception {
    var conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Woogle(), args));
  }

  public int run(String[] args) throws Exception {
    var key = "";
    try (var scanner = new Scanner(System.in)) {
      System.out.print("Please input a keyword:\n> ");
      key = scanner.nextLine().trim().toLowerCase(Locale.ROOT);
    }
    if (!key.isBlank()) {
      var indexPath = new Path(args[0]);
      searchAndPrint(key, indexPath);
    }
    return 0;
  }

  protected void searchAndPrint(String key, Path indexPath) throws IOException {
    var conf = getConf();
    var fs = FileSystem.get(conf);

    InverseDocumentFreq idf;
    var inverseDocumentFreqPath = new Path(indexPath, "inverse_document_freq");
    var filePath = new Path(inverseDocumentFreqPath, InvertedIndex.parseFilename(key));
    try (var reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
      var line = reader.readLine();
      idf = InverseDocumentFreq.parse(line);
      printInverseDocumentFreq(key, idf);
    } catch (FileNotFoundException e) {
      System.out.println(key + ": not found");
      return;
    }

    TermFreq tf;
    var partition = getPartition(key);
    var termFreqsPath = new Path(indexPath, String.format("part-r-%05d", partition));
    try (var reader = new BufferedReader(new InputStreamReader(fs.open(termFreqsPath)))) {
      var line = "";
      while ((line = reader.readLine()) != null) {
        var lineSplit = line.split("\t");
        var token = lineSplit[0];
        if (Objects.equals(key, token)) {
          try {
            tf = TermFreq.parse(lineSplit[1]);
            printInvertedIndex(tf, idf);
          } catch (Exception e) {
            log.warn(token + ": invalid index entry, error: " + e);
          }
        }
      }
    } catch (FileNotFoundException e) {
      log.error(key + ": index not exists");
    }
  }

  protected int getPartition(String key) {
    var textKey = new Text(key);
    return (textKey.hashCode() & Integer.MAX_VALUE) % Driver.NUM_REDUCE_TASKS;
  }

  private void printInverseDocumentFreq(String token, InverseDocumentFreq idf) {
    System.out.printf(
        "%s: IDF = %6f | found in %d files:\n",
        token,
        idf.inverseDocumentFreq(),
        idf.fileCount()
    );
  }

  private void printInvertedIndex(TermFreq tf, InverseDocumentFreq idf) {
    System.out.printf(
        "  %s: TF = %6e (%d times) | TF-IDF = %6e | positions:",
        tf.filename(),
        tf.termFreq(),
        tf.tokenCount(),
        tf.termFreq() * idf.inverseDocumentFreq()
    );
    for (var position : tf.positions()) {
      System.out.print(" ");
      System.out.print(position);
    }
    System.out.println();
  }
}
