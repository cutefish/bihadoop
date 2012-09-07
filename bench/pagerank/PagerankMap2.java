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
    float alpha;
    int blockSize;

    public void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      alpha = conf.getFloat("pagerank.alpha", 0.85f);
      blockSize = conf.getInt("pagerank.block.size", 1);
    }

    /**
     * Map.
     * input:
     * index format:
     * matrix mat + "\t" + blockColId + "\t" + blockRowId
     * vector vec + "\t" + blockRowId
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

      int matIdx = 0;
      int vecIdx = 0;
      if (indices[0].contains("mat")) {
        matIdx = 0;
        vecIdx = 1;
      }
      else {
        vecIdx = 0;
        matIdx = 1;
      }
      
      Segment[] segments = trSegs.segments;

      Segment matSeg = segments[matIdx];
      Segment vecSeg = segments[vecIdx];

      context.setStatus("mat: " + indices[matIdx] + "\n" +
                         "vec: " + indices[vecIdx] + "\n" +
                         "matSeg: " + matSeg + "\n" + 
                         "vecSeg: " + vecSeg);

      FSDataInputStream in;
      BufferedReader reader;

      context.setStatus("reading vector");

      HashMap<Integer, Double> prevRank = new HashMap<Integer, Double>();
      in = fs.openCachedReadOnly(vecSeg.getPath());
      in.seek(vecSeg.getOffset());
      reader = new BufferedReader(new InputStreamReader(in));
      int bytesRead = 0;
      while(bytesRead < vecSeg.getLength()) {
        String lineText = reader.readLine();
        if (lineText == null) break;
        bytesRead += lineText.length() + 1;
        //ignore comment
        if (lineText.startsWith("#")) continue;
        String[] line = lineText.split("\t");
        //ignore ill-formed lines
        try {
          int rowId = Integer.parseInt(line[0]);
          double rank = Double.parseDouble(line[1]);
          prevRank.put(rowId, rank);
          int blockRowId = rowId / blockSize;
          context.write(new Text("" + blockRowId),
                        new Text("" + rowId + "\t" + rank + "\tprev"));
        }
        catch(Exception e) {
          continue;
        }
      }
      in.close();

      context.setStatus("reading matrix");
      in = fs.openCachedReadOnly(matSeg.getPath());
      in.seek(matSeg.getOffset());
      reader = new BufferedReader(new InputStreamReader(in));
      bytesRead = 0;
      while (bytesRead < matSeg.getLength()) {
        String lineText = reader.readLine();
        if (lineText == null) break;
        bytesRead += lineText.length() + 1;
        trSegs.progress = (float) (in.getPos() - matSeg.getOffset()) / 
            (float) matSeg.getLength();

        //ignore comment
        if (lineText.startsWith("#")) continue;
        String[] line = lineText.split("\t");
        //ignore ill-formed lines
        int rowId, colId;
        double xferProb, partialRank;
        try {
          rowId = Integer.parseInt(line[0]);
          colId = Integer.parseInt(line[1]);
          xferProb = Double.parseDouble(line[2]);
          double rank = prevRank.get(colId);
          partialRank = xferProb * rank;
          int blockRowId = rowId / blockSize;
          context.write(new Text("" + blockRowId),
                        new Text("" + rowId + "\t" + partialRank));
        }
        catch (Exception e) {
          continue;
        }
      }
      in.close();
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
    double threshold = 0.0001;

    public void setup(Context context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        alpha = conf.getFloat("pagerank.alpha", 0.85f);
        numNodes = conf.getInt("pagerank.num.nodes", 1);
        threshold = (double) conf.getFloat("pagerank.converge.threshold", 
                                           0.0001f);
    }

    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context) 
        throws IOException, InterruptedException {
      TreeMap<Integer, Double> prevRank = new TreeMap<Integer, Double>();
      TreeMap<Integer, Double> currRank = new TreeMap<Integer, Double>();
      for (Text val : values) {
        String[] record = val.toString().split("\t");
        //if its a prev rank record
        int rowId = Integer.parseInt(record[0]);
        double recordRank = Double.parseDouble(record[1]);
        if (record.length == 3) {
          prevRank.put(rowId, recordRank);
        }
        else {
          Double rank = currRank.get(rowId);
          if (rank == null) {
            currRank.put(rowId, recordRank);
          }
          else {
            currRank.put(rowId, recordRank + rank);
          }
        }
      }

      StringBuilder sb = new StringBuilder();
      boolean reportedChange = false;
      for (Map.Entry<Integer, Double> entry : currRank.entrySet()) {
        int rowId = entry.getKey();
        double elemCurrRank = entry.getValue();
        double elemPrevRank = prevRank.get(entry.getKey());
        if (!reportedChange) {
          double diff = Math.abs(elemCurrRank - elemPrevRank);
          if (diff > threshold) {
            context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
            reportedChange = true;
          }
        }
        sb.append("" + rowId + "\t" + elemCurrRank + "\n");
      }

      context.write(new Text("vec\t" + key), 
                    new Text(sb.toString()));
    }
  }

  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path inPath = null;
  protected Path outPath = null;
  protected Path nodePath = null;
  protected Path edgePath = null;
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
    System.out.println("PagerankPrep <inPath> <outPath>");
    return -1;
  }

  public int run(final String[] args) throws Exception {
    if (args.length < 3) {
      return printUsage();
    }

    conf = getConf();
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    inPath = new Path(args[0]);
    outPath = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);

    edgePath = new Path(inPath.getParent(), "blkmatrix");
    nodePath = new Path(inPath.getParent(), "blkvector");
    fs.delete(edgePath, true);
    fs.delete(nodePath, true);
    String[] prepArgs = {inPath.toString(), 
      edgePath.toString(), nodePath.toString()};

    PagerankPrep.main(prepArgs);

    fs.delete(outPath, true);
    int maxNumIterations = conf.getInt("pagerank.max.num.iteration", 100);

    boolean converged = false;
    for (int i = 0; i < maxNumIterations; ++i) {
      //Every iteration we read from edgePath and nodePath and output to
      //outPath.
      Job job = waitForJobFinish(configStage1());
      Counters c = job.getCounters();
      long changed = c.findCounter(PrCounters.CONVERGE_CHECK).getValue();
      System.out.println("Iteration: " + " changed: " + changed);
      if (changed == 0) {
        System.out.println("Converged.");
        fs.delete(edgePath);
        fs.delete(nodePath);
        converged = true;
        break;
      }
      fs.delete(edgePath);
      fs.delete(nodePath);
      fs.rename(outPath, nodePath);
    }

    if (!converged) {
      System.out.println("Reached the max iteration.");
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

  private void waitForJobFinish(Job job) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new RuntimeException(job.toString());
    }
  }

  private Job configStage1() throws Exception {
    Job job = new Job(conf, "PagerankMap2");
    job.setJarByClass(PagerankMap2.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(2);
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

  public static class PagerankMap2OutputFormat<K, V>
        extends IndexedTextOutputFormat<K, V> {
    @Override
    protected <K, V> String generateIndexForKeyValue(
        K key, V value, String path) {
      return  key.toString();
    }
  }

}
