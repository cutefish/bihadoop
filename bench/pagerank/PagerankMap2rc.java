
package bench.pagerank;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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

public class PagerankMap2rc extends Configured implements Tool {

  protected static enum PrCounters { CONVERGE_CHECK }

  /**
   * Map reads a vector block and a matrix block and do the multiplication.
   *
   */

  public static class MapStage1
        extends Mapper<String[], TrackedSegments,
                        Text, BytesWritable> {
    FileSystem fs;
    int numRowBlocks;
    int numColBlocks;
    int numNodes;
    boolean useCache;

    public void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      numNodes = conf.getInt("pagerank.num.nodes", 1);
      numRowBlocks = conf.getInt("pagerank.num.row.blocks", -1);
      numColBlocks = conf.getInt("pagerank.num.col.blocks", -1);
      useCache = conf.getBoolean("pagerank.useCache", true);

      System.out.println("num nodes: " + numNodes);
      System.out.println("num row blocks: " + numRowBlocks);
      System.out.println("num col blocks: " + numColBlocks);
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

      int blockRowId = Integer.parseInt(indices[edgeIdx].split("\t")[1]);
      int blockColId = Integer.parseInt(indices[edgeIdx].split("\t")[2]);
      int left = numNodes - numColBlocks * (numNodes / numColBlocks);
      int colBlockSize = numNodes / numColBlocks + 
          ((blockColId < left) ? 1 : 0);
      left = numNodes - numRowBlocks * (numNodes / numRowBlocks);
      int rowBlockSize = numNodes / numRowBlocks + 
          ((blockRowId < left) ? 1 : 0);

      FSDataInputStream in;
      DataInputStream dataIn;

      Configuration conf = context.getConfiguration();
      long edgeVersionId = conf.getLong("pagerank.edge.versionId", 0);
      long nodeVersionId = conf.getLong("pagerank.node.versionId", 0);
      long start, end;

      context.setStatus("reading node vector: " + indices[nodeIdx] + 
                        " nodeSgmt: " + nodeSgmt);

      /**
       * We store the previous rank in a array then combine them into a byte
       * array, every node has a rank.
       */
      double[] prevRank = new double[colBlockSize];
      start = System.currentTimeMillis();
      if (useCache) {
        in = fs.openCachedReadOnly(nodeSgmt.getPath(), nodeVersionId);
      }
      else {
        in = fs.open(nodeSgmt.getPath());
      }
      in.seek(nodeSgmt.getOffset());
      dataIn = new DataInputStream(new BufferedInputStream(in));

      int bytesRead = 0;
      while(bytesRead < nodeSgmt.getLength()) {
        int rowId = dataIn.readInt();
        int idInBlock = rowId / numColBlocks;
        double rank = dataIn.readDouble();
        prevRank[idInBlock] = rank;
        bytesRead += (4 + 8);
      }
      //we add a header ahead of buffer to distinguish between previous and
      //current value
      ByteBuffer bbuf;
      if (blockRowId == 0) {
        bbuf = ByteBuffer.allocate(colBlockSize * 12 + 4);
        bbuf.put((byte)0x55);
        bbuf.put((byte)0x00);
        bbuf.put((byte)0x55);
        bbuf.put((byte)0x00);
        int count = 0;
        for (int i = blockColId; i < numNodes; i += numColBlocks) {
          bbuf.putInt(i);
          bbuf.putDouble(prevRank[count]);
          count ++;
        }
        context.write(new Text("" + blockColId), new BytesWritable(bbuf.array()));
      }
      in.close();
      end = System.currentTimeMillis();
      System.out.println("Processed node " + bytesRead + " byte in " + (end - start) + " ms");
      System.out.println("Node processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");

      // read edge matrix
      context.setStatus("reading edge matrix: " + indices[edgeIdx] + 
                        " edgeSgmt: " + edgeSgmt);
      double[] rankArray = new double[rowBlockSize];
      //identify zero xferProb to shrink the output size
      Arrays.fill(rankArray, -10.0);
      start = System.currentTimeMillis();
      if (useCache) {
        in = fs.openCachedReadOnly(edgeSgmt.getPath(), edgeVersionId);
      }
      else {
        in = fs.open(edgeSgmt.getPath());
      }
      in.seek(edgeSgmt.getOffset());
      dataIn = new DataInputStream(new BufferedInputStream(in));

      bytesRead = 0;
      while (bytesRead < edgeSgmt.getLength()) {
        int rowId, colId, colIdInBlock;
        double xferProb, partialRank;
        try {
          rowId = dataIn.readInt();
          colId = dataIn.readInt();
          xferProb = dataIn.readDouble();
          bytesRead += 4 + 4 + 8;
          colIdInBlock = colId / numColBlocks;
          double rank = prevRank[colIdInBlock];
          partialRank = xferProb * rank;

          int rowIdInBlock = rowId / numRowBlocks;
          if (rankArray[rowIdInBlock] < -1) {
            rankArray[rowIdInBlock] = partialRank;
          }
          else {
            rankArray[rowIdInBlock] += partialRank;
          }
        }
        catch (Exception e) {
          System.out.println("" + e + ", on bytesRead: " + bytesRead);
        }
      }
      in.close();

      HashMap<Integer, ByteArrayOutputStream> outMap = 
          new HashMap<Integer, ByteArrayOutputStream>();
      byte[] header = new byte[] { 
        (byte) 0xff, (byte)0x00, (byte)0xff, (byte)0x00 };

      int count = 0;
      for (int i = blockRowId; i < numNodes; i += numRowBlocks) {
        if (rankArray[count] < -1) {
          count ++;
          continue;
        }

        int newBlockId = i % numColBlocks;
        ByteArrayOutputStream out = outMap.get(newBlockId);
        if (out == null) {
          out = new ByteArrayOutputStream();
          out.write(header);
          outMap.put(newBlockId, out);
        }
        ByteBuffer tmpBuf = ByteBuffer.allocate(4 + 8);
        tmpBuf.putInt(i);
        tmpBuf.putDouble(rankArray[count]);
        out.write(tmpBuf.array());
        count ++;
      }

      //output ranks
      long writeStart = System.currentTimeMillis();
      long outSize = 0;
      for (Map.Entry<Integer, ByteArrayOutputStream> entry : outMap.entrySet()) {
        int blockId = entry.getKey();
        ByteArrayOutputStream out = entry.getValue();
        byte[] outBytes = out.toByteArray();
        outSize  += outBytes.length;
        context.write(new Text("" + blockId), new BytesWritable(outBytes));
      }
      end = System.currentTimeMillis();
      System.out.println("Processed edge " + bytesRead + " byte in " + (end - start) + " ms");
      System.out.println("Edge processing bandwidth: " + bytesRead / (end - start) / 1000 + " MByte/s");
      System.out.println("Write " + outSize + " byte in " + (end - writeStart) + " ms");
      System.out.println("Write bandwidth: " + outSize / (end - writeStart) / 1000 + " MByte/s");
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
          extends Reducer<Text, BytesWritable, Text, byte[]> {

    float alpha = 0;
    int numNodes = 0;
    int numColBlocks = 0;
    double threshold = 0.0001;
    boolean reportedChange = false;

    public void setup(Context context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        alpha = conf.getFloat("pagerank.alpha", 0.15f);
        numNodes = conf.getInt("pagerank.num.nodes", 1);
        numColBlocks = conf.getInt("pagerank.num.col.blocks", -1);
        threshold = (double) conf.getFloat("pagerank.converge.threshold", 
                                           0.0001f);
    }

    public void reduce(final Text key,
                       final Iterable<BytesWritable> values,
                       final Context context) 
        throws IOException, InterruptedException {

      int blockColId = Integer.parseInt(key.toString());
      int left = numNodes - numColBlocks * (numNodes / numColBlocks);
      int colBlockSize = numNodes / numColBlocks + 
          ((blockColId < left) ? 1 : 0);
      double[] prevRank = new double[colBlockSize];
      double[] currRank = new double[colBlockSize];
      int blockId = Integer.parseInt(key.toString());

      for (BytesWritable val : values) {
        ByteBuffer bbuf = ByteBuffer.wrap(val.getBytes());
        int length = val.getLength();
        System.out.println("key: " + key.toString() + 
                           " length: " + length);
        byte[] header = new byte[4];
        header[0] = bbuf.get();
        header[1] = bbuf.get();
        header[2] = bbuf.get();
        header[3] = bbuf.get();
        if ((header[0] == (byte) 0x55) &&
            (header[1] == (byte) 0x00) &&
            (header[2] == (byte) 0x55) &&
            (header[3] == (byte) 0x00)) {
          //previous rank
          int size = (length - 4) / 12;
          for (int i = 0; i < size; ++i) {
            int rowId = bbuf.getInt();
            double rank = bbuf.getDouble();
            int idInBlock = rowId / numColBlocks;
            prevRank[idInBlock] = rank;
          }
        }
        else if ((header[0] == (byte) 0xff) &&
                 (header[1] == (byte) 0x00) &&
                 (header[2] == (byte) 0xff) &&
                 (header[3] == (byte) 0x00)) {
          int size = (length - 4) / 12;
          for (int i = 0; i < size; ++i) {
            int rowId = bbuf.getInt();
            double rank = bbuf.getDouble();
            int idInBlock = rowId / numColBlocks;
            currRank[idInBlock] += rank;
          }
        }
        else {
          throw new IOException("Wrong byte header: " + val.getBytes().length + 
                                " key: " + key.toString());
        }
      }

      context.setStatus("Writing output");

      ByteBuffer bbuf = ByteBuffer.allocate(colBlockSize * (4 + 8));
      int total = currRank.length;
      int mileStore = total / 100;
      int count = 0;
      for (int i = blockId; i < numNodes; i += numColBlocks) {
        double xferProb = currRank[count];
        double elemPrevRank = prevRank[count];
        //add the coefficients
        //alpha / |G| + (1 - alpha) * rank
        double elemCurrRank = (1 - alpha) * xferProb + alpha / numNodes;
        bbuf.putInt(i);
        bbuf.putDouble(elemCurrRank);
        count ++;
        if (!reportedChange) {
          double diff = Math.abs(elemCurrRank - elemPrevRank);
          if (diff > threshold) {
            context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
            reportedChange = true;
          }
        }
        if ((mileStore != 0) && (count % mileStore == 0)) {
          context.progress();
        }
      }

      context.write(new Text("node\t" + key), bbuf.array());
    }
  }


  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path edgePath = null;
  protected Path blkEdgePath = null;
  protected Path initNodePath = null;
  protected Path nodePath = null;
  protected Path outPath = null;
  protected Configuration conf = null;
  protected String rxc = null;

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new PagerankMap2rc(),
                                        args);
      System.exit(result);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankMap2rc <edgePath> <outPath> [rxc]");
    return -1;
  }

  public int run(final String[] args) throws Exception {
    if (args.length < 2 || args.length > 4) {
      return printUsage();
    }

    conf = getConf();
    if (conf == null) {
      conf = new Configuration();
    }
    conf.addResource("pagerank-conf.xml");
    setConf(conf);
    checkValidity();

    edgePath = new Path(args[0]);
    outPath = new Path(args[1]);
    if (args.length == 3) {
      rxc = args[2];
    }
    FileSystem fs = FileSystem.get(conf);
    long start, end;

    blkEdgePath = new Path(edgePath.getParent(), "blkedge");
    initNodePath = new Path(edgePath.getParent(), "initialNodeRank");
    nodePath = new Path(edgePath.getParent(), "blknode");

    PagerankMap2rcPrep prepare = new PagerankMap2rcPrep();
    prepare.setPaths(edgePath.toString(), blkEdgePath.toString(),
                     initNodePath.toString());
    prepare.setRxC(this.rxc);

    if (conf.getBoolean("pagerank.initialize", true)) {
      fs.delete(initNodePath, true);
      fs.delete(blkEdgePath, true);
      System.out.println("Generating initial node");
      prepare.initNodes();
      System.out.println("done");
      System.out.println("Tranforming edges");
      start = System.currentTimeMillis();
      prepare.blockEdges();
      end = System.currentTimeMillis();
      System.out.println("===map2 experiment===<time>[PagerankMap2rcPrep]: " + 
                       (end - start) + " ms");
    }
    else {
      if (!fs.exists(initNodePath)) {
        System.out.println("Generating initial node");
        prepare.initNodes();
        System.out.println("done");
      }
      if (!fs.exists(blkEdgePath)) {
        System.out.println("Tranforming edges");
        start = System.currentTimeMillis();
        prepare.blockEdges();
        end = System.currentTimeMillis();
        System.out.println("===map2 experiment===<time>[PagerankMap2rcPrep]: " + 
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
      System.out.println("===map2 experiment===<iter time>[PagerankMap2rcIterative]: " + 
                         (iterEnd - iterStart) + " ms");
    }
    end = System.currentTimeMillis();
    System.out.println("===map2 experiment===<time>[PagerankMap2rcIterative]: " + 
                       (end - start) + " ms");

    if (!converged) {
      System.out.println("Reached the max iteration.");
      fs.rename(nodePath, outPath);
    }
    if (conf.getBoolean("pagerank.keep.intermediate", false)) {
      FileUtil.copy(fs, outPath, fs, nodePath, false, true, conf);
    }
    if (conf.getBoolean("pagerank.map2.toplaintxt", false)) {
      convertToPlainText();
    }
    return 1;
  }

  private void checkValidity() {
    int numRowBlocks = conf.getInt("pagerank.num.row.blocks", -1);
    if (numRowBlocks == -1) 
      throw new IllegalArgumentException("block size not set");
    int numColBlocks = conf.getInt("pagerank.num.col.blocks", -1);
    if (numColBlocks == -1) 
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
    Job job = new Job(conf, "PagerankMap2rcStage0");
    job.setJarByClass(PagerankMap2rc.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(byte[].class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, PagerankMap2Filter.class);
    Map2InputFormat.setInputPaths(job, blkEdgePath, initNodePath);
    job.setOutputFormatClass(PagerankMap2OutputFormat.class);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

  private Job configStage1() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankMap2rcStage1");
    job.setJarByClass(PagerankMap2rc.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(byte[].class);
    job.setInputFormatClass(Map2InputFormat.class);
    Map2InputFormat.setIndexFilter(job, PagerankMap2Filter.class);
    Map2InputFormat.setInputPaths(job, blkEdgePath, nodePath);
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
        extends IndexedByteArrayOutputFormat<K, V> {
    @Override
    protected <K, V> String generateIndexForKeyValue(
        K key, V value, String path) {
      return  key.toString();
    }
  }

  public void convertToPlainText() throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(outPath);
    Path plainTxtPath = new Path(outPath, "plaintxt");
    fs.delete(plainTxtPath);
    DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(fs.create(plainTxtPath)));
    for (FileStatus file : files) {
      Path path = file.getPath();
      System.out.println("converting file " + path);
      if (!path.toString().contains("part-r"))
        continue;
      if (path.toString().contains("map2idx"))
        continue;
      DataInputStream dataIn = new DataInputStream(
          new BufferedInputStream(fs.open(path)));
      try {
        while(true) {
          int rowId = dataIn.readInt();
          double rank = dataIn.readDouble();
          out.write(("" + rowId + "\t" + rank + "\n").getBytes());
        }
      }
      catch (EOFException eof) {
      }
    }
    out.close();
  }
}
