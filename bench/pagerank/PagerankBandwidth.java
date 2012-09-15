
package bench.pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
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

/**
 * Pagerank test bandwidth.
 *
 * Just read and do nothing.
 *
 */

public class PagerankBandwidth extends Configured implements Tool {

  /**
   * Map reads a vector block and a matrix block and do the multiplication.
   *
   */

  public static class MapStage1
        extends Mapper<String[], TrackedSegments,
                        Text, Text> {
    FileSystem fs;
    int blockSize;
    int numNodes;
    boolean useCache;

    public void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      numNodes = conf.getInt("pagerank.num.nodes", 1);
      blockSize = conf.getInt("pagerank.block.size", 1);
      useCache = conf.getBoolean("pagerank.useCache", true);
    }

    public void map(final String[] indices,
                    final TrackedSegments trSegs,
                    final Context context)
        throws IOException, InterruptedException {

      int edgeIdx = 0;
      int nodeIdx = 0;
      if (indices[0].contains("edge")) {
        edgeIdx = 0;
        nodeIdx = 1;
      }
      else {
        edgeIdx = 0;
        nodeIdx = 1;
      }
      
      Segment[] segments = trSegs.segments;

      Segment edgeSgmt = segments[edgeIdx];
      Segment nodeSgmt = segments[nodeIdx];

      FSDataInputStream in;
      BufferedReader reader;

      Configuration conf = context.getConfiguration();
      long edgeVersionId = conf.getLong("pagerank.edge.versionId", 0);
      long nodeVersionId = conf.getLong("pagerank.node.versionId", 0);
      long start, end;

      context.setStatus("reading node vector: " + indices[nodeIdx] + 
                        " nodeSgmt: " + nodeSgmt);

      int[] array = new int[4096];
      //HashMap<Integer, Double> prevRank = new HashMap<Integer, Double>();
      start = System.currentTimeMillis();
      if (useCache) {
        in = fs.openCachedReadOnly(nodeSgmt.getPath(), nodeVersionId);
      }
      else {
        in = fs.open(nodeSgmt.getPath());
      }
      in.seek(nodeSgmt.getOffset());
      reader = new BufferedReader(new InputStreamReader(in));
      int bytesRead = 0;
      while(bytesRead < nodeSgmt.getLength()) {
        String lineText = reader.readLine();
        if (lineText == null) break;
        bytesRead += lineText.length() + 1;
        //ignore comment and blank line
        if (lineText.startsWith("#")) continue;
        if (lineText.equals("")) continue;
        String[] line = lineText.split("\t");
      }
      in.close();
      end = System.currentTimeMillis();
      System.out.println("Processed node in " + (end - start) + " ms");
      System.out.println("Node processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");

      context.setStatus("reading edge matrix: " + indices[edgeIdx] + 
                        " edgeSgmt: " + edgeSgmt);
      start = System.currentTimeMillis();
      if (useCache) {
        in = fs.openCachedReadOnly(edgeSgmt.getPath(), edgeVersionId);
      }
      else {
        in = fs.open(edgeSgmt.getPath());
      }
      in.seek(edgeSgmt.getOffset());
      reader = new BufferedReader(new InputStreamReader(in));
      bytesRead = 0;
      while (bytesRead < edgeSgmt.getLength()) {
        String lineText = reader.readLine();
        if (lineText == null) break;
        bytesRead += lineText.length() + 1;
        //ignore comment and blank line
        if (lineText.startsWith("#")) continue;
        if (lineText.equals("")) continue;

        String[] line = lineText.split("\t");
        //context.write(new Text("" + 1),
        //              new Text(lineText));
      }
      in.close();
      end = System.currentTimeMillis();
      System.out.println("Processed edge in " + (end - start) + " ms");
      System.out.println("Edge processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");
    }
  }

  /**
   * Reduce.
   */
  public static class RedStage1
          extends Reducer<Text, Text, Text, Text> {

    float alpha = 0;
    int numNodes = 0;
    int blockSize = 0;
    double threshold = 0.0001;
    boolean reportedChange = false;

    public void setup(Context context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        alpha = conf.getFloat("pagerank.alpha", 0.15f);
        numNodes = conf.getInt("pagerank.num.nodes", 1);
        blockSize = conf.getInt("pagerank.block.size", 1);
        threshold = (double) conf.getFloat("pagerank.converge.threshold", 
                                           0.0001f);
    }

    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context) 
        throws IOException, InterruptedException {
        return;
    }
  }

  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path inPath = null;
  protected Path edgePath = null;
  protected Path outPath = null;
  protected Path initNodePath = null;
  protected Path nodePath = null;
  protected Configuration conf = null;

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new PagerankBandwidth(),
                                        args);
      System.exit(result);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankBandwidth <edgePath> <outPath>");
    return -1;
  }

  public int run(final String[] args) throws Exception {
    if (args.length != 2) {
      return printUsage();
    }

    conf = getConf();
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    inPath = new Path(args[0]);
    outPath = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    long start, end;

    edgePath = new Path(inPath.getParent(), "blkedge");
    initNodePath = new Path(inPath.getParent(), "initialNodeRank");
    nodePath = new Path(inPath.getParent(), "blknode");

    if (conf.getBoolean("pagerank.initialize", true)) {
      fs.delete(initNodePath, true);
      fs.delete(edgePath, true);
      System.out.println("Generating initial node");
      PagerankPrep.initNode(initNodePath);
      System.out.println("done");
      String[] prepArgs = {inPath.toString(), edgePath.toString()};
      System.out.println("Tranforming edges");
      start = System.currentTimeMillis();
      PagerankPrep.main(prepArgs);
      end = System.currentTimeMillis();
      System.out.println("===map2 experiment===<time>[PagerankPrep]: " + 
                       (end - start) + " ms");
    }
    else {
      if (!fs.exists(initNodePath)) {
        System.out.println("Generating initial node");
        PagerankPrep.initNode(initNodePath);
        System.out.println("done");
      }
      if (!fs.exists(edgePath)) {
        String[] prepArgs = {inPath.toString(), edgePath.toString()};
        System.out.println("Tranforming edges");
        start = System.currentTimeMillis();
        PagerankPrep.main(prepArgs);
        end = System.currentTimeMillis();
        System.out.println("===map2 experiment===<time>[PagerankPrep]: " + 
                           (end - start) + " ms");
      }
    }

    fs.delete(nodePath, true);
    fs.delete(outPath, true);
    int maxNumIterations = conf.getInt("pagerank.max.num.iteration", 100);

    System.out.println("Start iterating");
    boolean converged = false;
    start = System.currentTimeMillis();
    conf.setLong("pagerank.edge.versionId", start);
    for (int i = 0; i < maxNumIterations; ++i) {
      long iterStart = System.currentTimeMillis();
      Job job;
      conf.setLong("pagerank.node.versionId", start);
      //first iteration read from initNodePath
      job = waitForJobFinish(configStage0());
      long iterEnd = System.currentTimeMillis();
      System.out.println("===map2 experiment===<iter time>[PagerankBandwidthIterative]: " + 
                         (iterEnd - iterStart) + " ms");
      fs.delete(outPath, true);
    }
    end = System.currentTimeMillis();
    System.out.println("===map2 experiment===<time>[PagerankBandwidthIterative]: " + 
                       (end - start) + " ms");

    if (!converged) {
      System.out.println("Reached the max iteration.");
      fs.rename(nodePath, outPath);
    }
    if (conf.getBoolean("pagerank.keep.intermediate", false)) {
      FileUtil.copy(fs, outPath, fs, nodePath, false, true, conf);
    }
    return 1;
  }

  private void checkValidity() {
    int blockSize = conf.getInt("pagerank.block.size", -1);
    if (blockSize == -1) 
      throw new IllegalArgumentException("block size not set");
    int numNodes = conf.getInt("pagerank.num.nodes", -1);
    if (numNodes == -1) 
      throw new IllegalArgumentException("number of nodes not set");
  }

  private Job waitForJobFinish(Job job) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new RuntimeException(job.toString());
    }
    return job;
  }

  private Job configStage0() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankBandwidthStage0");
    job.setJarByClass(PagerankBandwidth.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, PagerankBWFilter.class);
    Map2InputFormat.setInputPaths(job, edgePath, initNodePath);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

  private Job configStage1() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankBandwidthStage1");
    job.setJarByClass(PagerankBandwidth.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, PagerankBWFilter.class);
    Map2InputFormat.setInputPaths(job, edgePath, nodePath);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

  public static class PagerankBWFilter implements Map2Filter {
    public boolean accept(String idx0, String idx1) {
      String edgeIdx;
      String nodeIdx;
      if (idx0.contains("edge")) {
        edgeIdx = idx0;
        nodeIdx = idx1;
      }
      else {
        edgeIdx = idx1;
        nodeIdx = idx0;
      }
      String[] edgeId = edgeIdx.split("\t");
      String[] nodeId = nodeIdx.split("\t");
      if (edgeId.length != 3 || nodeId.length != 2) return false;
      try {
        int edgeColId = Integer.parseInt(edgeId[2]);
        int nodeRowId = Integer.parseInt(nodeId[1]);
        if (edgeColId == nodeRowId) return true;
      }
      catch(Exception e) {
        return false;
      }
      return false;
    }
  }

}
