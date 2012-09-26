
package bench.matmul;

import java.io.BufferedReader;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.map2.IndexedTextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.*;

public class MatMulIOMap2 {
  public static class MapStage
        extends Mapper<String[], TrackedSegments, Text, BytesWritable> {

    FileSystem fs;
    int numRowsInBlock;
    int numColsInBlock;
    boolean useCache;

    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      numRowsInBlock = conf.getInt("matmul.num.rows.in.block", 1);
      numColsInBlock = conf.getInt("matmul.num.cols.in.block", 1);
      useCache = conf.getBoolean("matmul.useCache", true);
    }

    public void map(final String[] indices,
                    final TrackedSegments trSegs,
                    final Context context) 
        throws IOException, InterruptedException {

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

      FSDataInputStream AIn;
      FSDataInputStream BIn;
      DataInputStream dataAIn;
      DataInputStream dataBIn = null;
      
      Configuration conf = context.getConfiguration();
      long versionId = conf.getLong("matmul.versionId", 0);
      
      long start, end;

      //do the multiplication
      if (useCache) {
        AIn = fs.openCachedReadOnly(segA.getPath(), versionId);
        BIn = fs.openCachedReadOnly(segB.getPath(), versionId);
      }
      else {
        AIn = fs.open(segA.getPath());
        BIn = fs.open(segB.getPath());
      }
      AIn.seek(segA.getOffset());
      dataAIn = new DataInputStream(new BufferedInputStream(AIn));

      ByteBuffer outbuf = ByteBuffer.allocate(
          numRowsInBlock * numRowsInBlock * 8);
      long readATime = 0, readBTime = 0;
      for (int i = 0; i < numRowsInBlock; ++i) {
        double[] rowA = new double[numColsInBlock];
        start = System.currentTimeMillis();
        for (int j = 0; j < numColsInBlock; ++j) {
          rowA[j] = dataAIn.readDouble();
        }
        end = System.currentTimeMillis();
        readATime += end - start;

        BIn.seek(segB.getOffset());
        dataBIn = new DataInputStream(new BufferedInputStream(BIn));
        start = System.currentTimeMillis();
        for (int j = 0; j < numRowsInBlock; ++j) {

          double sum = 0;
          for (int k = 0; k < numColsInBlock; ++k) {
            sum += dataBIn.readDouble() * rowA[k];
          }
          context.setStatus("finished read row: " + j);
          outbuf.putDouble(sum);
        }
        end = System.currentTimeMillis();
        readBTime += end - start;
      }

      dataAIn.close();
      dataBIn.close();

      System.out.println("matrixA read time: " + readATime + " ms");
      System.out.println("matrixA read bandwidth: " + 
                         numRowsInBlock*numColsInBlock*8 / readATime / 1000 + 
                         " MBytes/s");
      System.out.println("matrixB read time: " + readBTime + " ms");
      System.out.println("matrixB read bandwidth: " + 
                         numRowsInBlock*numColsInBlock*8 / readBTime / 1000 + 
                         " MBytes/s");

      //prepare for context write
      String[] Aindices = indices[AIdx].split("_");
      String[] Bindices = indices[BIdx].split("_");
      String rowIdx = Aindices[2];
      String colIdx = Bindices[4];

        context.write(new Text(rowIdx + "\t" + colIdx),
                      new BytesWritable(outbuf.array()));
    }
  }

  public static class RedStage
        extends Reducer<Text, BytesWritable, Text, byte[]> {

    int numRowsInBlock;

    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      numRowsInBlock = conf.getInt("matmul.num.rows.in.block", 1);
    }

    public void reduce(final Text key,
                       final Iterable<BytesWritable> values,
                       final Context context)
        throws IOException, InterruptedException {

      int size = numRowsInBlock * numRowsInBlock;
      double[] out = new double[size];
      ByteBuffer outBuf = ByteBuffer.allocate(size *8);
      for (BytesWritable val: values) {
        ByteBuffer buf = ByteBuffer.wrap(val.getBytes());
        outBuf.rewind();
        for (int i = 0; i < size; ++i) {
          outBuf.mark();
          double curr = outBuf.getDouble();
          curr += buf.getDouble();
          outBuf.reset();
          outBuf.putDouble(curr);
        }
      }

      context.write(key, outBuf.array());
    }
  }

  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path inPath = null;
  protected Path outPath = null;
  Configuration conf;

  public static void main(final String[] args) {
    try {
      MatMulIOMap2 mmm = new MatMulIOMap2();
      mmm.run(args);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("MatMulIOMap2 <inPath> <outPath>");
      System.exit(-1);
    }
    inPath = new Path(args[0]);
    outPath = new Path(args[1]);
    conf = new Configuration();
    conf.addResource("matmul-conf.xml");
    FileSystem fs = FileSystem.get(conf);
    long start, end;

    //prepare
    if ((conf.getBoolean("matmul.initialize", true)) ||
        (!fs.exists(inPath))) {
      MatMulPrep prep = new MatMulPrep();
      prep.setPath(inPath);
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
    Job job = new Job(conf, "MatMulIOMap2");
    job.setJarByClass(MatMulIOMap2.class);
    job.setMapperClass(MapStage.class);
    job.setReducerClass(RedStage.class);
    job.setNumReduceTasks(numReducers);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(byte[].class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setFileNameAsIndex(job);
    Map2InputFormat.setIndexFilter(job, MatMulMap2Filter.class);
    Map2InputFormat.setInputPaths(job, inPath);
    job.setOutputFormatClass(MatMulMap2OutputFormat.class);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }


  public static class MatMulMap2Filter implements Map2Filter {
    public boolean accept(String idx0, String idx1) {
      String AIdx = null, BIdx = null;
      if (idx0.contains("A")) AIdx = idx0;
      if (idx0.contains("B")) BIdx = idx0;
      if (idx1.contains("A")) AIdx = idx1;
      if (idx1.contains("B")) BIdx = idx1;

      if ((AIdx == null) || (BIdx == null)) return false;

      String[] Aindices = AIdx.split("_");
      String[] Bindices = BIdx.split("_");
      if (Aindices.length != 5 || Bindices.length != 5) return false;
      try {
        int AColId = Integer.parseInt(Aindices[4]);
        int BRowId = Integer.parseInt(Bindices[2]);
        if (AColId == BRowId) return true;
      }
      catch(Exception e) {
        return false;
      }
      return false;
    }
  }

  public static class MatMulMap2OutputFormat<K, V>
        extends IndexedByteArrayOutputFormat<K, V> {
    @Override
    protected <K, V> String generateIndexForKeyValue(
        K key, V value, String path) {
      return  key.toString();
    }
  }

}
