package bench.pagerank;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.map2.IndexedTextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.map2.*;
/**
 * Prepare the adjacency matrix for pagerank.
 *
 * Do a transposed, column normalized, blocked transformation on the matrix.
 *
 * The input format is:
 * src_id + "\t" + dst_id "\n"
 *
 * The output has two files, one regular matrix file and an index file
 * The format of matrix file is:
 * dst_id + "\t" + src_id + "\t" + norm_val + "\n"
 *
 * The format of index file is
 * IDX_START | index | SEG_START | offset | len | IDX_END
 */
public class PagerankMap2rcPrep extends Configured implements Tool {

  /****************************************************************************
   * Stage 1.
   * Normalize and transpose the matrix
   ***************************************************************************/
  public static class MapStage1
        extends Mapper<Object, Text, Text, Text> {

    public void map(final Object key, final Text value,
                    final Context context)
        throws IOException, InterruptedException {

      String lineText = value.toString();
      //ignore comment
      if (lineText.startsWith("#")) return;

      final String[] line = lineText.split("\t");
      //ignore ill-formed lines
      if (line.length != 2) return;

      //key = srcId, value = dstId
      context.write(new Text(line[0]), new Text(line[1]));
    }
  }

  public static class ReduceStage1
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context)
        throws IOException, InterruptedException {

      //The probability of reaching a dstId node from srcId is
      //numOfEdgesFromSrcId ^ (-1)
      ArrayList<String> dstNodeList = new ArrayList<String>();
      for (Text val: values) {
        dstNodeList.add(val.toString());
      }
      float prob = 0;
      if (dstNodeList.size() > 0) 
        prob = 1 / (float)dstNodeList.size();

      for (String val: dstNodeList) {
        context.write(new Text(val), 
                      new Text(key.toString() + "\t" + prob));
      }
    }
  }

  /****************************************************************************
   * Stage 2.
   * Block the matrix
   ***************************************************************************/
  public static class MapStage2
        extends Mapper<Object, Text, Text, Text> {

    int numNodes;
    int numRowBlocks;
    int numColBlocks;
    
    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", -1);
      numRowBlocks = conf.getInt("pagerank.num.row.blocks", -1);
      numColBlocks = conf.getInt("pagerank.num.col.blocks", -1);
    }

    public void map(final Object key, final Text value,
                    final Context context)
        throws IOException, InterruptedException {

      String lineText = value.toString();
      //ignore comment
      if (lineText.startsWith("#")) return;

      final String[] line = lineText.split("\t");
      //ignore ill-formed lines
      if (line.length != 3) {
        System.out.println("Ill-formed line: " + lineText);
        return;
      }

      int rowId = Integer.parseInt(line[0]);
      int colId = Integer.parseInt(line[1]);
      int blockRowId = rowId % numRowBlocks;
      int blockColId = colId % numColBlocks;

      Text newKey = new Text("edge" + "\t" + blockRowId + "\t" + blockColId + 
                             "\t" + numRowBlocks);

      //key = blockId, value = value
      context.write(newKey, value);
    }
  }

  public static class ReduceStage2
        extends Reducer<Text, Text, Text, byte[]> {

    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context)
        throws IOException, InterruptedException {

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      for (Text val: values) {
        String lineText = val.toString();
        String[] line = lineText.split("\t");
        int dstId = Integer.parseInt(line[0]);
        int srcId = Integer.parseInt(line[1]);
        double prob = Double.parseDouble(line[2]);
        ByteBuffer bbuf = ByteBuffer.allocate(4 + 4 + 8);
        bbuf.putInt(dstId);
        bbuf.putInt(srcId);
        bbuf.putDouble(prob);
        out.write(bbuf.array());
      }

      String[] keyStrings = key.toString().split("\t");
      Text newKey = new Text(keyStrings[0] + "\t" + keyStrings[1] + "\t" + 
                             keyStrings[2]);
      context.write(newKey, out.toByteArray());
    }
  }

  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path edgePath = null;
  protected Path blkEdgePath = null;
  protected Path blkNodePath = null;
  protected Path tmpPath = null;
  protected Configuration conf = null;
  protected String rxc = "null";

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new PagerankMap2rcPrep(),
                                        args);
      System.out.println("PagerankMap2rcPrep main return: " + result);
      return;
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankMap2rcPrep " + 
                       "<edgePath> <blkedgePath> <initNodePath>" + 
                       "[rxc]");
    return -1;
  }

  public void setPaths(String edge, String blkEdge, String blkNode) {
		edgePath = new Path(edge);
		blkEdgePath = new Path(blkEdge);
    blkNodePath = new Path(blkNode);
  }
  
  public void setRxC(String rxc) {
    if (rxc == null) return;
    this.rxc = rxc;
  }

  public int run(final String[] args) throws Exception {
    if (args.length < 3 || args.length > 5) {
      return printUsage();
    }

    conf = getConf();
    if (conf == null) {
      conf = new Configuration();
    }
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    setPaths(args[0], args[1], args[2]);
    if (args.length == 4) {
      setRxC(args[3]);
    }
    blockEdges();
    initNodes();

    return 1;
  }

  static class edgeFileFilter implements PathFilter, Configurable {

    private Configuration conf;
    private String rxc = "null";

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.rxc = conf.get("input.path.filter.rxc");
    }

    //file name ...socljrc
    public boolean accept(Path path) {
      if (rxc.equals("null")) {
        System.out.println("choosing all. ");
        return true;
      }
      if (path.toString().endsWith("soclj")) return true;
      try {
        int R = Integer.parseInt(rxc.split("x")[0]);
        int C = Integer.parseInt(rxc.split("x")[1]);
        //extract the last two digit
        String s = path.toString();
        int r = Integer.parseInt(s.substring(s.length() - 2, s.length() - 1));
        int c = Integer.parseInt(s.substring(s.length() - 1));
        if (r < R && c < C) {
          System.out.println("path: " + path.toString());
          return true;
        }
        return false;
      }
      catch (Exception e) {
        return false;
      }
    }
  }

  public int blockEdges() throws Exception {
    conf = getConf();
    if (conf == null) {
      conf = new Configuration();
      setConf(conf);
    }
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    FileSystem fs = FileSystem.get(conf);
    tmpPath = new Path(edgePath.getParent(), "tmp");
    fs.delete(blkEdgePath, true);
    fs.delete(tmpPath, true);

    waitForJobFinish(configStage1());
    waitForJobFinish(configStage2());

    fs.delete(tmpPath, true);

    return 1;
  }

  public void initNodes() throws Exception {
    conf = getConf();
    if (conf == null) {
      conf = new Configuration();
      setConf(conf);
    }
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    FileSystem fs = FileSystem.get(conf);
    fs.delete(blkNodePath, true);

    int numColBlocks = conf.getInt("pagerank.num.col.blocks", 1);
    int numNodes = conf.getInt("pagerank.num.nodes", 1);
    Path[] localPath = {
      new Path("/tmp/initialNodeRank"), 
      new Path("/tmp/initialNodeRank.map2idx")
    };

    BufferedOutputStream out = new BufferedOutputStream(
        new FileOutputStream(localPath[0].toString()));
    DataOutputStream idxOut = new DataOutputStream(
        new FileOutputStream(localPath[1].toString()));

    int prevOff = 0;
    int currOff = 0;

    int left = numNodes - numColBlocks * (numNodes / numColBlocks);
    for (int i = 0; i < numColBlocks; ++i) {
      String blockId = "node" + "\t" + i;
      int actualBlockSize = numNodes / numColBlocks + ((i < left) ? 1 : 0);
      int numBytes = actualBlockSize * (4 + 8);
      //allocate one int and one double for each element
      ByteBuffer bbuf = ByteBuffer.allocate(numBytes);
      for (int j = i; j < numNodes; j += numColBlocks) {
        bbuf.putInt(j);
        bbuf.putDouble(1.0 / numNodes);
      }
      out.write(bbuf.array());
      currOff += numBytes;
      idxOut.write(IndexingConstants.IDX_START);
      Text.writeString(idxOut, blockId);
      idxOut.write(IndexingConstants.SEG_START);
      idxOut.writeLong(prevOff);
      idxOut.writeLong(currOff - prevOff);
      idxOut.write(IndexingConstants.IDX_END);
      prevOff = currOff;
      System.out.print(".");
    }
    out.close();
    idxOut.close();
    System.out.println("\n");

    //copy to hdfs
    fs = FileSystem.get(conf);
    fs.mkdirs(blkNodePath);
    fs.copyFromLocalFile(false, true, localPath, blkNodePath);
  }

    
  private void checkValidity() {
    int numRowBlocks = conf.getInt("pagerank.num.row.blocks", -1);
    if (numRowBlocks == -1) 
      throw new IllegalArgumentException("num of row blocks not set");
    int numColBlocks = conf.getInt("pagerank.num.col.blocks", -1);
    if (numColBlocks == -1) 
      throw new IllegalArgumentException("num of col blocks not set");
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

  private Job configStage1() throws Exception {
    conf.set("input.path.filter.rxc", this.rxc);
    Job job = new Job(conf, "PagerankMap2rcPrep");
    job.setJarByClass(PagerankMap2rcPrep.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(ReduceStage1.class);
    job.setNumReduceTasks(conf.getInt("pagerank.num.reducers", 1));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, edgePath);
    FileInputFormat.setInputPathFilter(job, edgeFileFilter.class);
    FileOutputFormat.setOutputPath(job, tmpPath);
    return job;
  }
  
  private Job configStage2() throws Exception {
    Job job = new Job(conf, "PagerankMap2rcPrepStage2");
    job.setJarByClass(PagerankMap2rcPrep.class);
    job.setMapperClass(MapStage2.class);
    job.setReducerClass(ReduceStage2.class);
    job.setNumReduceTasks(conf.getInt("pagerank.num.reducers", 1));
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(byte[].class);
    job.setOutputFormatClass(MatBlockOutputFormat.class);
    job.setPartitionerClass(PagerankMap2Partitioner.class);
    FileInputFormat.setInputPaths(job, tmpPath);
    FileOutputFormat.setOutputPath(job, blkEdgePath);
    return job;
  }

  public static class MatBlockOutputFormat<K, V>
        extends IndexedByteArrayOutputFormat<K, V> {
    @Override
    protected <K, V> String generateIndexForKeyValue(
        K key, V value, String path) {
      return  key.toString();
    }
  }

  public static class PagerankMap2Partitioner<K, V>
        extends Partitioner<K, V> {

    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
      try {
        String[] keyStrings = key.toString().split("\t");
        int blockRowId = Integer.parseInt(keyStrings[1]);
        int blockColId = Integer.parseInt(keyStrings[2]);
        int numBlocks = Integer.parseInt(keyStrings[3]);
        return (blockRowId * numBlocks + blockColId) % numReduceTasks;
      }
      catch (Exception e) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
      }
    }
  }

}
