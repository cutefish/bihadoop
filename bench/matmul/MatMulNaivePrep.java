
package bench.matmul;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.map2.*;

public class MatMulNaivePrep {

  int numARowBlocks;
  int numBColBlocks;
  int numRowsInBlock;
  int numColsInBlock;
  Path outAPath;
  Path outBPath;

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("java MatMulNaivePrep <outAPath> <outBPath>");
      System.exit(-1);
    }

    Path outAPath = new Path(args[0]);
    Path outBPath = new Path(args[1]);

    MatMulNaivePrep prep = new MatMulNaivePrep();
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

    System.out.println("Generating matrices");

    //gen two matrix
    //A matrix
    //A matrix use regular file to be used inside each map.
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
    //B matrix use sequence file format to be treated as input
    //1 * numBColBlocks blocks
    //numColsInBlock * numRowsInBlock block size
    for (int i = 0; i < numBColBlocks; ++i) {
      ByteBuffer keyBuf = ByteBuffer.allocate(4);
      keyBuf.putInt(i);
      ByteBuffer valBuf = ByteBuffer.allocate(blockSize * 8);
      for (int j = 0; j < numRowsInBlock; ++j) {
        for (int k = 0; k < numColsInBlock; ++k) {
          valBuf.putDouble((double)i / (double)numColsInBlock);
        }
      }
      Path blockOutPath = new Path(outBPath, "B_c_" + i);
      SequenceFile.setCompressionType(conf, SequenceFile.CompressionType.NONE);
      SequenceFile.Writer out = SequenceFile.createWriter(
          fs, conf, blockOutPath, BytesWritable.class, BytesWritable.class);
      SequenceFileAsBinaryOutputFormat.WritableValueBytes wvaluebytes = 
          new SequenceFileAsBinaryOutputFormat.WritableValueBytes();
      wvaluebytes.reset(new BytesWritable(valBuf.array()));
      out.appendRaw(keyBuf.array(), 0, 4, wvaluebytes);
      wvaluebytes.reset(null);
      //out.append(new BytesWritable(keyBuf.array()),
      //           new BytesWritable(valBuf.array()));
      System.out.print(".");
      out.close();
    }
    System.out.println("\n");
  }

}
