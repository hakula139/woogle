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
import xyz.hakula.woogle.model.InverseDocumentFreq;
import xyz.hakula.woogle.model.TermFreq;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
      search(key, indexPath);
    }
    return 0;
  }

  protected void search(String key, Path indexPath) throws IOException {
    var conf = getConf();
    var fs = FileSystem.get(conf);
    var partition = getPartition(key);
    var filePath = new Path(indexPath, String.format("part-r-%05d", partition));

    try (var reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
      var tfs = new ArrayList<TermFreq>();
      var line = "";

      while ((line = reader.readLine()) != null) {
        var lineSplit = line.split("\t");
        var token = lineSplit[0];
        var entry = lineSplit[1];

        if (Objects.equals(key, token)) {
          try {
            if (entry.charAt(0) != '$') {
              tfs.add(TermFreq.parse(entry));
            } else {
              print(key, InverseDocumentFreq.parse(entry), tfs);
              return;
            }
          } catch (Exception e) {
            log.warn(token + ": invalid index entry, error: " + e);
          }
        }
      }
    } catch (FileNotFoundException e) {
      log.error(key + ": index not exists");
    }

    System.out.println(key + ": not found");
  }

  protected int getPartition(String key) {
    var textKey = new Text(key);
    return (textKey.hashCode() & Integer.MAX_VALUE) % Driver.NUM_REDUCE_TASKS;
  }

  protected void print(String token, InverseDocumentFreq idf, ArrayList<TermFreq> tfs) {
    System.out.printf(
        "%s: IDF = %6f | found in %d file%s:\n",
        token,
        idf.inverseDocumentFreq(),
        idf.fileCount(),
        idf.fileCount() == 1 ? "" : "s"
    );

    for (var tf : tfs) {
      System.out.printf(
          "  %s: TF = %6e (%d time%s) | TF-IDF = %6e | positions:",
          tf.filename(),
          tf.termFreq(),
          tf.tokenCount(),
          tf.tokenCount() == 1 ? "" : "s",
          tf.termFreq() * idf.inverseDocumentFreq()
      );
      int limit = 10;
      for (var position : tf.positions()) {
        System.out.print(" ");
        System.out.print(position);
        if (--limit == 0) {
          System.out.print(" ...");
          break;
        }
      }
      System.out.println();
    }
  }
}
