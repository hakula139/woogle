package xyz.hakula;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.Scanner;

public class Woogle {
  private static final String INPUT_PATH = "output";

  public static void main(String[] args) {
    var scanner = new Scanner(System.in);
    System.out.print("Please input a keyword:\n> ");
    var token = scanner.nextLine().trim();
    var partition = getPartition(token);
    var inputPath = new Path(INPUT_PATH, String.format("part-r-%05d", partition));
    System.out.println("Result for " + token + ":");
  }

  private static int getPartition(String key) {
    var textKey = new Text(key);
    return (textKey.hashCode() & Integer.MAX_VALUE) % Driver.NUM_REDUCE_TASKS;
  }
}
