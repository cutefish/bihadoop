package bench.matmul;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.*;

public class MatMulMap2 {

  public static class MapStage
        extends Mapper<String[], TrackedSegments, 
                        NullWritable, NullWritable> {

    FileSystem fs;
    int numRowsInBlock;
    int numColsInBlock;
    boolean useCache;

    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      int blockSize = conf.getInt("matmul.block.size", 1);
      numRowsInBlock = conf.getInt("matmul.num.rows.in.block", 1);
      numColsInBlock = blockSize / numRowsInBlock;
      useCache = conf.getBoolean("matmul.useCache", true);
    }

    public void map(final String[] indices,
                    final TrackedSegments trSegs,
                    final Context context) 
        throws IOException, InterruptedException {
      
      System.out.println("working on: " + indices[0] + "\t" + indices[1]);

      int AIdx = 0, BIdx = 0;
      if (indices[0].contains("A")) {
        AIdx = 0; BIdx = 1;
      }
      else {
        AIdx = 1; BIdx = 0;
      }

      Segment[] segments = trSegs.segments;
      Segment segA = segments[AIdx];
      Segment segB = segments[BIdx];

      FSDataInputStream in;
      DataInputStream dataIn;

      Configuration conf = context.getConfiguration();
      long versionId = conf.getLong("matmul.versionId", 0);
      
      long start, end;

      //read the B segment into memory
      int size = numRowsInBlock * numColsInBlock;
      double[] matrixBlockB = new double[size];
      start = System.currentTimeMillis();
      if (useCache) {
        in = fs.openCachedReadOnly(segB.getPath(), versionId);
      }
      else {
        in = fs.open(segB.getPath());
      }
      in.seek(segB.getOffset());
      dataIn = new DataInputStream(new BufferedInputStream(in));
      for (int i = 0; i < size; ++i) {
        matrixBlockB[i] = dataIn.readDouble();
      }
      in.close();
      end = System.currentTimeMillis();
      System.out.println("matrixB read time: " + (end - start) + " ms");
      System.out.println("matrixB read bandwidth: " + 
                         size * 8 / (end - start) / 1000 + " MBytes/s");

      //prepare for context write
      String CRowId = indices[AIdx].split("_")[2];
      String CColId = indices[BIdx].split("_")[2];
      String outName = "C_" + CRowId + "_" + CColId;
      Path outPath = new Path(FileOutputFormat.getWorkOutputPath(context), 
                              outName);
      BufferedOutputStream out = new BufferedOutputStream(
          fs.create(outPath));
      DataOutputStream dataOut = new DataOutputStream(out);

      //do the multiplication
      if (useCache) {
        in = fs.openCachedReadOnly(segA.getPath(), versionId);
      }
      else {
        in = fs.open(segA.getPath());
      }
      in.seek(segA.getOffset());
      dataIn = new DataInputStream(new BufferedInputStream(in));
      long readTime = 0, calcTime = 0;
      for (int i = 0; i < numRowsInBlock; ++i) {
        double[] rowA = new double[numColsInBlock];
        start = System.currentTimeMillis();
        for (int j = 0; j < numColsInBlock; ++j) {
          rowA[j] = dataIn.readDouble();
        }
        end = System.currentTimeMillis();
        readTime += end - start;

        //caclulate out[i, :]
        start = System.currentTimeMillis();
        for (int j = 0; j < numRowsInBlock; ++j) {
          //calculate out [i, j]
          double sum = 0;
          for (int k = 0; k < numColsInBlock; ++k) {
            sum += rowA[k] * matrixBlockB[j * numColsInBlock + k];
          }
          dataOut.writeDouble(sum);
        }
        end = System.currentTimeMillis();
        calcTime += end - start;
      }
      in.close();
      dataOut.close();
      System.out.println("matrixA read time: " + readTime + " ms");
      System.out.println("matrixA read bandwidth: " + 
                         size * 8 / readTime / 1000 + " MBytes/s");
      System.out.println("multiplication calc time: " + calcTime + " ms");
    }

  }

  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path inPath = null;
  protected Path APath = null;
  protected Path BPath = null;
  protected Path outPath = null;
  Configuration conf;

  public static void main(final String[] args) {
    try {
      MatMulMap2 mmm = new MatMulMap2();
      mmm.run(args);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("MatMulMap2 <inPath> <outPath>");
      System.exit(-1);
    }
    inPath = new Path(args[0]);
    APath = new Path(inPath, "A");
    BPath = new Path(inPath, "B");
    outPath = new Path(args[1]);
    conf = new Configuration();
    conf.addResource("matmul-conf.xml");
    FileSystem fs = FileSystem.get(conf);
    long start, end;

    //prepare
    if ((conf.getBoolean("matmul.initialize", true)) ||
        (!fs.exists(inPath))) {
      MatMulMap2Prep prep = new MatMulMap2Prep();
      prep.setPath(APath, BPath);
      prep.run();
    }

    fs.delete(outPath);

    start = System.currentTimeMillis();
    conf.setLong("matmul.versionId", start);
    waitForJobFinish(configStage());
    end = System.currentTimeMillis();

    System.out.println("===map2 experiment===<time>[MatMulMap2]: " + 
                       (end - start) + " ms");
  }

  private Job waitForJobFinish(Job job) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new RuntimeException(job.toString());
    }
    return job;
  }

  private Job configStage() throws Exception {
    int numReducers = conf.getInt("matmul.num.reducers", 1);
    Job job = new Job(conf, "MatMulMap2");
    job.setJarByClass(MatMulMap2.class);
    job.setMapperClass(MapStage.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setFileNameAsIndex(job);
    Map2InputFormat.setIndexFilter(job, MatMulMap2Filter.class);
    Map2InputFormat.setInputPaths(job, "" + APath + "," + BPath);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }


  public static class MatMulMap2Filter implements Map2Filter {
    public boolean accept(String idx0, String idx1) {
      if ((idx0.contains("A") && idx1.contains("B")) ||
          (idx0.contains("B") && idx1.contains("A"))) {
        return true;
      }
      return false;
    }
  }

}
