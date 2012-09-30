
package bench.matmul;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.*;

public class MatMulNaive {

  public static class MapStage
        extends Mapper<BytesWritable, BytesWritable, 
                        NullWritable, NullWritable> {

    int numRowsInBlock;
    int numColsInBlock;

    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int blockSize = conf.getInt("matmul.block.size", 1);
      numRowsInBlock = conf.getInt("matmul.num.rows.in.block", 1);
      numColsInBlock = blockSize / numRowsInBlock;
    }

    public void map(final BytesWritable key,
                    final BytesWritable value,
                    final Context context) 
        throws IOException, InterruptedException {

      System.out.println("start map"); 

      ByteBuffer buf;
      buf = ByteBuffer.wrap(key.getBytes());
      int rowBlockId = buf.getInt();
      System.out.println("Block B: " + rowBlockId);
      int size = numRowsInBlock * numColsInBlock;
      buf = ByteBuffer.wrap(value.getBytes());
      buf.mark();

      long start, end;

      //loop through all the A matrix and compute
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      Path inPath = new Path(conf.get("matmul.in.path"));
      FileStatus[] files = fs.listStatus(inPath);

      for (FileStatus file : files) {
        Path blockPath = file.getPath();
        System.out.println("calculate block: " + blockPath);
        //prepare for input
        FSDataInputStream in = fs.open(blockPath);
        DataInputStream dataIn = new DataInputStream(
            new BufferedInputStream(in));
        //prepare for context write
        String CRowId = "" + rowBlockId;
        String CColId = blockPath.toString().split("_")[2];
        String outName = "C_" + CRowId + "_" + CColId;
        Path outPath = new Path(FileOutputFormat.getWorkOutputPath(context), 
                                outName);
        BufferedOutputStream out = new BufferedOutputStream(
            fs.create(outPath));
        DataOutputStream dataOut = new DataOutputStream(out);

        //do the multiplication
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
          buf.reset();
          for (int j = 0; j < numRowsInBlock; ++j) {
            //calculate out [i, j]
            double sum = 0;
            for (int k = 0; k < numColsInBlock; ++k) {
              sum += rowA[k] * buf.getDouble();
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
      MatMulNaive mmn = new MatMulNaive();
      mmn.run(args);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("MatMulNaive <inPath> <outPath>");
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
      MatMulNaivePrep prep = new MatMulNaivePrep();
      prep.setPath(APath, BPath);
      prep.run();
    }

    fs.delete(outPath);

    conf.set("matmul.in.path", APath.toString());
    start = System.currentTimeMillis();
    waitForJobFinish(configStage());
    end = System.currentTimeMillis();

    System.out.println("===map2 experiment===<time>[MatMulNaive]: " + 
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
    Job job = new Job(conf, "MatMulNaive");
    job.setJarByClass(MatMulNaive.class);
    job.setMapperClass(MapStage.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    FileInputFormat.setInputPaths(job, BPath);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

}
