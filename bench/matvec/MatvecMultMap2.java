package bench.matvec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

    public void setup(Context context) {
      fs = FileSystem.get(context.getConfiguration());
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
      FSDataInputStream in;

      HashMap<Integer, Double> vecBlock = new HashMap<Integer, Double>();
      //build vector block into memory
      in = fs.openCachedReadOnly(vecSeg.getPath());
      in.seek(vecSeg.getOffset());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      while(in.getPos() < vecSeg.getOffset() + vecSeg.getLength()) {
        String lineText = reader.readLine();
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

      //calculate multiplication for each mat block element
      //assuming sorted in advance to save memory
      in = fs.openCachedReadOnly(matSeg.getPath());
      in.seek(blockSeg.getOffset());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      while(in.getPos() < vecSeg.getOffset() + vecSeg.getLength()) {
        String lineText = reader.readLine();
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
        context.write(IntWritable(rowId), DoubleWritable(rank));

        trSegs.progress = (double) (in.getPos() - vecSeg.getOffset()) / 
            (double) vecSeg.getLength();
      }
    }
  }

  public static class RedStage1
        extends Reducer<IntWritable, DoubleWritable, 
                        IntWritable, DoubleWritable> {

    public void reduce(final IntWritable key, 
                       final Iterable<DoubleWritable> values,
                       final Context context) {
      double sum;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      context.write(key, DoubleWritable(sum));
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // command line interface
  /////////////////////////////////////////////////////////////////////////////
  protected Configuration conf = getConf();
  protected int vecSize = 0;
  protected int blockSize = 0;
  protected Path matPath = null;
  protected Path vecPath = null;
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
    System.out.println("MatvecMultMap2 " + 
                       "<mat_path> <vec_path> <out_path>");

    ToolRunner.printGenericCommandUsage(System.out);

    return -1;
  }

  public int run(final String[] args) throws Exception {
    if (args.length != 4) {
      return printUsage();
    }

    conf.addResource("matvecmultmap2-conf.xml");
    vecSize = conf.getInt("matvecmultmap2.vec.size", -1);
    blockSize = conf.getInt("matvecmultmap2.block.size", -1);

    matPath = args[0];
    vecPath = args[1];
    outPath = args[2];

    FileSystem fs = FileSystem.get(conf);
    fs.delete(vecPath);
    fs.delete(outPath);

    //generate vector first
    generateVector(vecPath);

  }

  private void generateVector(Path vecPath) {
    FileWriter file = new FileWriter("/tmp/xyu40Vector");
    BufferedWriter out = new BufferedWriter(file);
    FileWriter idxFile = new FileWriter("/tmp/xyu40Vector.map2idx");
    BufferedWriter idxOut = new BufferedWriter(file);
    int prevBlockId = -1;
    int currBlockId = 0;
    long prevOff = 0;
    long currOff = 0;
    for (int i = 0; i < vecSize; ++i) {
      out.write("" + i + "\t" + 0.1);
      currBlockId = i / blockSize;
      if (currBlockId != prevBlockId) {

      }
    }
  }


}
