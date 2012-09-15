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
 * Pagerank map2 block implementation.
 *
 * P(n) = alpha * (1 / |G|) + (1 - alpha) * sum(P(m) / C(m))
 *
 * If we reorganize the (src, dst) adjacency matrix into a transposed column
 * normalized matrix, then
 *
 * P(n) = alpha * (1 / |G|) + (1 - alpha) * sum(M(n, m) * P'(m))
 *
 */

public class PagerankMap2 extends Configured implements Tool {

  protected static enum PrCounters { CONVERGE_CHECK }

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

    /**
     * Map.
     * input:
     * index format:
     * matrix "edge" + "\t" + blockColId + "\t" + blockRowId
     * vector "node" + "\t" + blockRowId
     *
     * segments:
     * matrix block: rowId + "\t" + colId + "\t" + xferProb
     * vector block: rowId + "\t" + rank
     *
     * output:
     * key: blockRowId + "\t" + rowId
     * value: rowId + "\t" + partialRank
     *
     * key: blockRowId + "\t" + rowId
     * value: "prev" + "\t" + rowId + "\t" + prevRank
     */
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

      //HashMap<Integer, Double> prevRank = new HashMap<Integer, Double>();
      int arraySize = (int) (blockSize * 1.5);
      double[] prevRank = new double[arraySize];
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
        //ignore ill-formed lines
        try {
          int rowId = Integer.parseInt(line[0]);
          int rowIdInBlock = rowId / (numNodes / blockSize);
          double rank = Double.parseDouble(line[1]);
          //prevRank.put(rowId, rank);
          prevRank[rowIdInBlock] = rank;
          //int blockRowId = rowId / blockSize;
          int blockRowId = rowId % (numNodes / blockSize);
          context.write(new Text("" + blockRowId),
                        new Text("" + rowId + "\t" + rank + "\tprev"));
        }
        catch(Exception e) {
          System.out.println("" + e + ", on line: " + lineText);
        }
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
        trSegs.progress = (float) (in.getPos() - edgeSgmt.getOffset()) / 
            (float) edgeSgmt.getLength();

        //ignore comment and blank
        if (lineText.startsWith("#")) continue;
        if (lineText.equals("")) continue;

        String[] line = lineText.split("\t");
        //ignore ill-formed lines
        int rowId, colId, colIdInBlock;
        double xferProb, partialRank;
        try {
          rowId = Integer.parseInt(line[0]);
          colId = Integer.parseInt(line[1]);
          xferProb = Double.parseDouble(line[2]);
          //Double rank = prevRank.get(colId);
          //if (rank == null) {
          //  continue;
          //}
          colIdInBlock = colId / (numNodes / blockSize);
          double rank = prevRank[colIdInBlock];
          partialRank = xferProb * rank;
          //int blockRowId = rowId / blockSize;
          int blockRowId = rowId % (numNodes / blockSize);
          context.write(new Text("" + blockRowId),
                        new Text("" + rowId + "\t" + partialRank));
        }
        catch (Exception e) {
          System.out.println("" + e + ", on line: " + lineText);
        }
      }
      in.close();
      end = System.currentTimeMillis();
      System.out.println("Processed edge in " + (end - start) + " ms");
      System.out.println("Edge processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");
    }
  }

  /**
   * Reduce.
   * Input:
   * key: blockRowId 
   * value: rowId + "\t" + rank
   * value: rowId + "\t" + rank + "\t" + "prev"
   *
   * Output:
   * key: blockRowId for index
   * value: rowId + "\t" + rank; ...
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
      //HashMap<Integer, Double> prevRank = new HashMap<Integer, Double>();
      //HashMap<Integer, Double> currRank = new HashMap<Integer, Double>();
      int arraySize = (int) (blockSize * 1.5);
      double[] prevRank = new double[arraySize];
      double[] currRank = new double[arraySize];
      int[] rowIdArray = new int[arraySize];
      for (Text val : values) {
        String[] record = val.toString().split("\t");
        //if its a prev rank record
        int rowId = Integer.parseInt(record[0]);
        int rowIdInBlock = rowId / (numNodes / blockSize);
        double recordRank = Double.parseDouble(record[1]);
        rowIdArray[rowIdInBlock] = rowId;
        if (record.length == 3) {
          //prevRank.put(rowId, recordRank);
          prevRank[rowIdInBlock] = recordRank;
        }
        else {
          //Double rank = currRank.get(rowId);
          //if (rank == null) {
          //  currRank.put(rowId, recordRank);
          //}
          //else {
          //  currRank.put(rowId, recordRank + rank);
          //}
          currRank[rowIdInBlock] += recordRank;
        }
      }

      StringBuilder sb = new StringBuilder();
      int total = currRank.length;
      int mileStore = total / 100;
      int count = 0;
      //for (Map.Entry<Integer, Double> entry : currRank.entrySet()) {
      for (int i = 0; i < currRank.length; ++i) {
        count ++;
        //int rowId = entry.getKey();
        //Double elemPrevRank = prevRank.get(rowId);
        //if (elemPrevRank == null) {
        //  throw new NullPointerException("PrevRank for id: " + rowId + 
        //                                 " not found");
        //}
        //double xferProb = entry.getValue();
        double xferProb = currRank[i];
        double elemPrevRank = prevRank[i];
        //add the coefficients
        //alpha / |G| + (1 - alpha) * rank
        double elemCurrRank = (1 - alpha) * xferProb + alpha / numNodes;
        if (!reportedChange) {
          double diff = Math.abs(elemCurrRank - elemPrevRank);
          if (diff > threshold) {
            context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
            reportedChange = true;
          }
        }
        sb.append("" + rowIdArray[i] + "\t" + elemCurrRank + "\n");
        if ((mileStore != 0) && (count % mileStore == 0)) {
          context.progress();
        }
      }

      context.write(new Text("node\t" + key), 
                    new Text(sb.toString()));
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
                                        new PagerankMap2(),
                                        args);
      System.exit(result);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankMap2 <edgePath> <outPath>");
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
      conf.setLong("pagerank.node.versionId", start + i);
      if (i == 0) {
        //first iteration read from initNodePath
        job = waitForJobFinish(configStage0());
      }
      else {
        //Every iteration we read from edgePath and nodePath and output to
        //outPath.
        job = waitForJobFinish(configStage1());
      }
      Counters c = job.getCounters();
      long changed = c.findCounter(PrCounters.CONVERGE_CHECK).getValue();
      System.out.println("Iteration: " + i + " changed: " + changed);
      if (changed == 0) {
        System.out.println("Converged.");
        fs.delete(nodePath);
        converged = true;
        break;
      }
      fs.delete(nodePath);
      fs.rename(outPath, nodePath);
      long iterEnd = System.currentTimeMillis();
      System.out.println("===map2 experiment===<iter time>[PagerankMap2Iterative]: " + 
                         (iterEnd - iterStart) + " ms");
    }
    end = System.currentTimeMillis();
    System.out.println("===map2 experiment===<time>[PagerankMap2Iterative]: " + 
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
    Job job = new Job(conf, "PagerankMap2Stage0");
    job.setJarByClass(PagerankMap2.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, PagerankMap2Filter.class);
    Map2InputFormat.setInputPaths(job, edgePath, initNodePath);
    job.setOutputFormatClass(PagerankMap2OutputFormat.class);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

  private Job configStage1() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankMap2Stage1");
    job.setJarByClass(PagerankMap2.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, PagerankMap2Filter.class);
    Map2InputFormat.setInputPaths(job, edgePath, nodePath);
    job.setOutputFormatClass(PagerankMap2OutputFormat.class);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

  public static class PagerankMap2Filter implements Map2Filter {
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

  public static class PagerankMap2OutputFormat<K, V>
        extends IndexedTextOutputFormat<K, V> {
    @Override
    protected <K, V> String generateIndexForKeyValue(
        K key, V value, String path) {
      return  key.toString();
    }
  }

}
