package bench.matvec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

public class MatvecMultMap2 extends Configured implements Tool {

  public static class MapStage1 
        extends Mapper<String[], TrackedSegments, 
                        IntWritable, DoubleWritable> {

    FileSystem fs;
    private boolean useCache = true;


    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      useCache = conf.getBoolean("matvec.useCache", true);
      System.out.println("set up: useCache:" + useCache);
    }

    public void map(final String[] indices,
                    final TrackedSegments trSegs,
                    final Context context) 
        throws IOException, InterruptedException {

      int matIdx = 0;
      int vecIdx = 0;
      for (int i = 0; i < indices.length; ++i) {
        if (indices[i].contains("mat")) {
          matIdx = i;
          break;
        }
      }
      for (int i = 0; i < indices.length; ++i) {
        if (indices[i].contains("vec")) {
          vecIdx = i;
          break;
        }
      }
      
      Segment[] segments = trSegs.segments;

      Segment matSeg = segments[matIdx];
      Segment vecSeg = segments[vecIdx];

      System.out.println("mat: " + indices[matIdx] + "\n" +
                         "vec: " + indices[vecIdx] + "\n" +
                         "matSeg: " + matSeg + "\n" + 
                         "vecSeg: " + vecSeg);

      FSDataInputStream in;
      BufferedReader reader;

      System.out.println("Start reading vector");

      HashMap<Integer, Double> vecBlock = new HashMap<Integer, Double>();
      //build vector block into memory
      if (useCache) {
        in = fs.openCachedReadOnly(vecSeg.getPath());
      }
      else {
        in = fs.open(vecSeg.getPath());
      }
      in.seek(vecSeg.getOffset());
      reader = new BufferedReader(new InputStreamReader(in));
      int bytesRead = 0;
      while(bytesRead < vecSeg.getLength()) {
        String lineText = reader.readLine();
        if (lineText == null) break;
        bytesRead += lineText.length() + 1;
        System.out.println(lineText + ": " + bytesRead);
        //ignore comment
        if (lineText.startsWith("#"))
          continue;
        String[] line = lineText.split("\t");
        //ignore ill-formed lines
        if (line.length != 2)
          continue;
        int rowId = Integer.parseInt(line[0]);
        double rank = Double.parseDouble(line[1]);
        vecBlock.put(rowId, rank);
      }
      in.close();
      
      System.out.println("Start reading matrix");
      //calculate multiplication for each mat block element
      //assuming sorted in advance to save memory
      if (useCache) {
        in = fs.openCachedReadOnly(matSeg.getPath());
      }
      else {
        in = fs.open(matSeg.getPath());
      }
      in.seek(matSeg.getOffset());
      reader = new BufferedReader(new InputStreamReader(in));
      bytesRead = 0;
      while(bytesRead < matSeg.getLength()) {
        String lineText = reader.readLine();
        if (lineText == null) break;
        bytesRead += lineText.length() + 1;
        System.out.println(lineText + ": " + bytesRead);
        //ignore comment
        if (lineText.startsWith("#"))
          continue;
        String[] line = lineText.split("\t");
        //ignore ill-formed lines
        if (line.length != 2)
          continue;
        int rowId = Integer.parseInt(line[0]);
        int colId = Integer.parseInt(line[1]);
        double rank = vecBlock.get(colId);
        context.write(new IntWritable(rowId), new DoubleWritable(rank));

        trSegs.progress = (float) (in.getPos() - matSeg.getOffset()) / 
            (float) matSeg.getLength();
      }
      in.close();
    }
  }

  public static class RedStage1
        extends Reducer<IntWritable, DoubleWritable, 
                        IntWritable, DoubleWritable> {

    public void reduce(final IntWritable key, 
                       final Iterable<DoubleWritable> values,
                       final Context context) 
        throws IOException, InterruptedException {
      double sum = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      context.write(key, new DoubleWritable(sum));
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // command line interface
  /////////////////////////////////////////////////////////////////////////////
  protected Configuration conf;
  protected Path inPath = null;
  protected Path outPath = null;

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new MatvecMultMap2(), 
                                        args);
      System.exit(result);
    }
    catch(Exception e) {
      System.out.println("error: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  // Print the command-line usage text.
  protected static int printUsage ()
  {
    System.out.println("MatvecMultMap2 <inPath> <outPath>");

    ToolRunner.printGenericCommandUsage(System.out);

    return -1;
  }

  public int run(final String[] args) throws Exception {
    if (args.length != 2) {
      return printUsage();
    }

    conf = getConf();
    conf.addResource("matvec-conf.xml");
    System.out.println("reading config done");

    inPath = new Path(args[0]);
    outPath = new Path(args[1]);

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outPath);

    //generate data
    generateVector();
    generateMatrix();

    //run
    safeRunJob(configStage1(), "configStage1");

    return 1;
  }

  private void generateVector() throws Exception {
    Path[] localPath = {
      new Path("/tmp/testVector"), 
      new Path("/tmp/testVector.map2idx")
    };
    FileOutputStream file = new FileOutputStream(localPath[0].toString());
    DataOutputStream out = new DataOutputStream(file);
    FileOutputStream idxFile = new FileOutputStream(localPath[1].toString());
    DataOutputStream idxOut = new DataOutputStream(idxFile);
    int vecSize = conf.getInt("matvec.node.size", 10);
    int blockSize = conf.getInt("matvec.block.size", 2);
    HashMap<String, List<String>> blocks = new HashMap<String, List<String>>();
    for (int i = 0; i < vecSize; ++i) {
      String line = "" + i + "\t" + 0.1 + "\n";
      String blockId = "vec\t" + i / blockSize;
      List<String> block = blocks.get(blockId);
      if (block == null) {
        block = new ArrayList<String>();
        blocks.put(blockId, block);
      }
      block.add(line);
    }

    int prevOff = 0;
    int currOff = 0;
    for (String block : blocks.keySet()) {
      for (String line : blocks.get(block)) {
        out.writeBytes(line);
        currOff += line.length();
      }
      idxOut.write(IndexingConstants.IDX_START);
      Text.writeString(idxOut, block);
      idxOut.write(IndexingConstants.SEG_START);
      idxOut.writeLong(prevOff);
      idxOut.writeLong(currOff - prevOff);
      idxOut.write(IndexingConstants.IDX_END);
      prevOff = currOff;
    }

    //copy to hdfs
    FileSystem fs = FileSystem.get(conf);
    fs.copyFromLocalFile(false, true, localPath, inPath);
  }

  private void generateMatrix() throws Exception {
    Path[] localPath = {
      new Path("/tmp/testMatrix"), 
      new Path("/tmp/testMatrix.map2idx")
    };
    FileOutputStream file = new FileOutputStream(localPath[0].toString());
    DataOutputStream out = new DataOutputStream(file);
    FileOutputStream idxFile = new FileOutputStream(localPath[1].toString());
    DataOutputStream idxOut = new DataOutputStream(idxFile);
    int matSize = conf.getInt("matvec.node.size", 10);
    int blockSize = conf.getInt("matvec.block.size", 2);
    float sparseLevel = conf.getFloat("matvec.sparse.level", 0.3f);
    HashMap<String, List<String>> blocks = new HashMap<String, List<String>>();
    Random r = new Random();
    for (int i = 0; i < matSize; ++i) {
      for (int j = 0; j < matSize; ++j) {
        if (r.nextFloat() > sparseLevel) continue;
        String line = "" + i + "\t" + j + "\n";
        String blockId = "mat\t" + i / blockSize + "\t" + j / blockSize;
        List<String> block = blocks.get(blockId);
        if (block == null) {
          block = new ArrayList<String>();
          blocks.put(blockId, block);
        }
        block.add(line);
      }
    }
    int prevOff = 0;
    int currOff = 0;
    for (String block : blocks.keySet()) {
      for (String line : blocks.get(block)) {
        out.writeBytes(line);
        currOff += line.length();
      }
      idxOut.write(IndexingConstants.IDX_START);
      Text.writeString(idxOut, block);
      idxOut.write(IndexingConstants.SEG_START);
      idxOut.writeLong(prevOff);
      idxOut.writeLong(currOff - prevOff);
      idxOut.write(IndexingConstants.IDX_END);
      prevOff = currOff;
    }

    //copy to hdfs
    FileSystem fs = FileSystem.get(conf);
    fs.copyFromLocalFile(false, true, localPath, inPath);
  }

  private Job safeRunJob(Job job, String message) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      System.out.println("Job: " + message + " failed");
      System.exit(-1);
      }
    return job;
  }

  private Job configStage1() throws Exception {
    Job job = new Job(conf, "MatvecMultMap2Stage1");
    job.setJarByClass(MatvecMultMap2.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, MatvecMultFilter.class);
    Map2InputFormat.setInputPaths(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

  public static class MatvecMultFilter implements Map2Filter {
    public boolean accept(String idx0, String idx1) {
      String matIdx;
      String vecIdx;
      if (idx0.contains("mat")) {
        matIdx = idx0;
        vecIdx = idx1;
      }
      else {
        matIdx = idx1;
        vecIdx = idx0;
      }
      String[] matId = matIdx.split("\t");
      String[] vecId = vecIdx.split("\t");
      if (matId.length != 3 || vecId.length != 2) return false;
      try {
        int matColId = Integer.parseInt(matId[2]);
        int vecRowId = Integer.parseInt(vecId[1]);
        if (matColId == vecRowId) return true;
      }
      catch(Exception e) {
        return false;
      }
      return false;
    }
  }
}
