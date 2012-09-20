
package bench.datagen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ExpandAdjFile extends DataGenerator {

  String inFile;
  String outFile;
  int srcShift;
  int dstShift;
  int numNodes;

  public void printUsage() {
    System.out.println("ExpandAdjFile <inFile> <outFile>" + 
                       " <srcShift> <dstShift> <numNodes>");
  }

  public void parseArgs(String[] args) throws Exception {
    if (args.length != 5) {
      printUsage();
      throw new RuntimeException("invalid argment");
    }
    inFile = args[0];
    outFile = args[1];
    srcShift = Integer.parseInt(args[2]);
    dstShift = Integer.parseInt(args[3]);
    numNodes = Integer.parseInt(args[4]);
  }

  private void genFile() throws IOException {
    BufferedReader r = new BufferedReader(new FileReader(inFile));
    BufferedWriter w = new BufferedWriter(new FileWriter(outFile));
    while(true) {
      String line = r.readLine();
      if (line == null) break;
      if (line.startsWith("#")) {
        w.write(line);
        w.newLine();
        continue;
      }
      String[] edge = line.split("\t");
      if (edge.length != 2) continue;
      try {
        int srcId = Integer.parseInt(edge[0]);
        int dstId = Integer.parseInt(edge[1]);
        srcId += srcShift * numNodes;
        dstId += dstShift * numNodes;
        w.write("" + srcId + "\t" + dstId + "\n");
      }
      catch (NumberFormatException nfe) {
      }
    }
    r.close();
    w.close();
  }

  public void generate() throws Exception {
    genFile();
  }

}
