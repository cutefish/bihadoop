package bench.matmul;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.map2.*;

public class MatMulPrep {

  int numRowBlocks;
  int numColBlocks;
  int numRowsInBlock;
  int numColsInBlock;
  Path outPath;

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("java MatMulPrep <outPath>");
      System.exit(-1);
    }

    Path outPath = new Path(args[0]);

    MatMulPrep prep = new MatMulPrep();
    prep.setPath(outPath);
    try {
      prep.run();
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void setPath(Path outPath) {
    this.outPath = outPath;
  }

  public void run() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource("matmul-conf.xml");

    numRowBlocks = conf.getInt("matmul.num.row.blocks", 1);
    numColBlocks = conf.getInt("matmul.num.col.blocks", 1);
    numRowsInBlock = conf.getInt("matmul.num.rows.in.block", 1);
    numColsInBlock = conf.getInt("matmul.num.cols.in.block", 1);

    FileSystem fs = FileSystem.get(outPath.toUri(), conf);
    fs.delete(outPath);
    fs.mkdirs(outPath);

    Random rand = new Random();

    //gen two matrix
    for (int i = 0; i < 2; ++i) {
      //foreach block
      for (int j = 0; j < numRowBlocks; ++j) {
        for (int k = 0; k < numColBlocks; ++k) {
          Path blockOutPath;
          if (i == 0) {
            blockOutPath = new Path(outPath, "A_r_" + j + "_c_" + k);
          }
          else {
            blockOutPath = new Path(outPath, "B_r_" + k + "_c_" + j);
          }
          BufferedOutputStream out = new BufferedOutputStream(
              fs.create(blockOutPath));
          DataOutputStream dataOut = new DataOutputStream(out);
          //foreach element
          for (int r = 0; r < numRowsInBlock; ++r) {
            for (int s = 0; s < numColsInBlock; ++s) {
              dataOut.writeDouble(rand.nextDouble());
            }
          }
          dataOut.close();
        }
      }
    }

  }

}
