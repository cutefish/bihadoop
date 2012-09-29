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

public class MatMulMap2Prep {

  int numARowBlocks;
  int numBColBlocks;
  int numRowsInBlock;
  int numColsInBlock;
  Path outAPath;
  Path outBPath;

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("java MatMulMap2Prep <outAPath> <outBPath>");
      System.exit(-1);
    }

    Path outAPath = new Path(args[0]);
    Path outBPath = new Path(args[1]);

    MatMulMap2Prep prep = new MatMulMap2Prep();
    prep.setPath(outAPath, outBPath);
    try {
      prep.run();
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void setPath(Path outAPath, Path outBPath) {
    this.outAPath = outAPath;
    this.outBPath = outBPath;
  }

  public void run() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource("matmul-conf.xml");

    numARowBlocks = conf.getInt("matmul.num.a.row.blocks", 1);
    numBColBlocks = conf.getInt("matmul.num.b.col.blocks", 1);
    int blockSize = conf.getInt("matmul.block.size", 1);
    numRowsInBlock = conf.getInt("matmul.num.rows.in.block", 1);
    numColsInBlock = blockSize / numRowsInBlock;

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outAPath);
    fs.mkdirs(outAPath);
    fs.delete(outBPath);
    fs.mkdirs(outBPath);

    Random rand = new Random();

    System.out.println("Generating matrices");

    //gen two matrix
    //A matrix
    //numARowBlocks * 1 blocks
    //numRowsInBlock * numColsInBlock block size
    for (int i = 0; i < numARowBlocks; ++i) {
      Path blockOutPath = new Path(outAPath, "A_r_" + i);
      BufferedOutputStream out = new BufferedOutputStream(
          fs.create(blockOutPath));
      DataOutputStream dataOut = new DataOutputStream(out);
      for (int j = 0; j < numRowsInBlock; ++j) {
        for (int k = 0; k < numColsInBlock; ++k) {
          dataOut.writeDouble((double)i / (double)numColsInBlock);
        }
      }
      System.out.print(".");
      dataOut.close();
    }
    //B matrix 
    //1 * numBColBlocks blocks
    //numColsInBlock * numRowsInBlock block size
    for (int i = 0; i < numBColBlocks; ++i) {
      Path blockOutPath = new Path(outBPath, "B_c_" + i);
      BufferedOutputStream out = new BufferedOutputStream(
          fs.create(blockOutPath));
      DataOutputStream dataOut = new DataOutputStream(out);
      for (int j = 0; j < numRowsInBlock; ++j) {
        for (int k = 0; k < numColsInBlock; ++k) {
          dataOut.writeDouble((double)i / (double)numColsInBlock);
        }
      }
      System.out.print(".");
      dataOut.close();
    }
    System.out.println("\n");
  }

}
