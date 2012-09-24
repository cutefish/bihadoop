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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
public class PagerankMap2Prep extends Configured implements Tool {

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

    int blockSize = 1;
    int numNodes = 1;
    
    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", -1);
      blockSize = conf.getInt("pagerank.block.size", -1);
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
      int blockRowId = rowId %(numNodes / blockSize);
      int blockColId = colId %(numNodes / blockSize);
      //int blockRowId = rowId / blockSize;
      //int blockColId = colId / blockSize;

      Text newKey = new Text("edge" + "\t" + blockRowId + "\t" + blockColId);

      //key = blockId, value = value
      context.write(newKey, value);
    }
  }

  static class MatComparator implements Comparator {
    public int compare(Object o1, Object o2) {
      String s1 = o1.toString();
      String s2 = o2.toString();

      String[] Id1 = s1.split("\t");
      String[] Id2 = s2.split("\t");
      if (Id1.length != 3) return 1;
      if (Id2.length != 3) return -1;
      int rowId1 = Integer.parseInt(Id1[0]);
      int rowId2 = Integer.parseInt(Id2[0]);
      int colId1 = Integer.parseInt(Id1[1]);
      int colId2 = Integer.parseInt(Id2[1]);
      if (rowId1 != rowId2) return rowId1 - rowId2;
      return colId1 - colId2;
    }
  }

  public static class ReduceStage2
        extends Reducer<Text, Text, Text, byte[]> {

    int blockSize = 1;
    MatComparator mc = new MatComparator();

    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      blockSize = conf.getInt("pagerank.block.size", -1);
    }

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

      context.write(key, out.toByteArray());
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

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new PagerankMap2Prep(),
                                        args);
      System.out.println("PagerankMap2Prep main return: " + result);
      return;
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankMap2Prep " + 
                       "<edgePath> <blkedgePath> <initNodePath>");
    return -1;
  }

  public void setPaths(String edge, String blkEdge, String blkNode) {
		edgePath = new Path(edge);
		blkEdgePath = new Path(blkEdge);
    blkNodePath = new Path(blkNode);
  }

  public int run(final String[] args) throws Exception {
    if (args.length != 3) {
      return printUsage();
    }

    conf = getConf();
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    setPaths(args[0], args[1], args[2]);
    blockEdges();
    initNodes();

    return 1;
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
    Configuration conf = new Configuration();
    if (conf == null) {
      conf = new Configuration();
      setConf(conf);
    }
    conf.addResource("pagerank-conf.xml");
    checkValidity(conf);

    FileSystem fs = FileSystem.get(conf);
    fs.delete(blkNodePath, true);

    int blockSize = conf.getInt("pagerank.block.size", 1);
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

    int numBlocks = numNodes / blockSize;
    int left = numNodes - numBlocks * (numNodes / numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      String blockId = "node" + "\t" + i;
      int actualBlockSize = numNodes / numBlocks + ((i < left) ? 1 : 0);
      int numBytes = actualBlockSize * (4 + 8);
      //allocate one int and one double for each element
      ByteBuffer bbuf = ByteBuffer.allocate(numBytes);
      for (int j = i; j < numNodes; j += numBlocks) {
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
    int blockSize = conf.getInt("pagerank.block.size", -1);
    if (blockSize == -1) 
      throw new IllegalArgumentException("block size not set");
    int numNodes = conf.getInt("pagerank.num.nodes", -1);
    if (numNodes == -1) 
      throw new IllegalArgumentException("number of nodes not set");
  }

  private static void checkValidity(Configuration conf) {
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

  private Job configStage1() throws Exception {
    Job job = new Job(conf, "PagerankMap2Prep");
    job.setJarByClass(PagerankMap2Prep.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(ReduceStage1.class);
    job.setNumReduceTasks(conf.getInt("pagerank.num.reducers", 1));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, edgePath);
    FileOutputFormat.setOutputPath(job, tmpPath);
    return job;
  }
  
  private Job configStage2() throws Exception {
    Job job = new Job(conf, "PagerankMap2PrepStage2");
    job.setJarByClass(PagerankMap2Prep.class);
    job.setMapperClass(MapStage2.class);
    job.setReducerClass(ReduceStage2.class);
    job.setNumReduceTasks(conf.getInt("pagerank.num.reducers", 1));
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(byte[].class);
    job.setOutputFormatClass(MatBlockOutputFormat.class);
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

  //private static void genInitNodeRanks(Configuration conf,
  //                                     Path outPath) throws Exception {
  //  int blockSize = conf.getInt("pagerank.block.size", 1);
  //  int numNodes = conf.getInt("pagerank.num.nodes", 1);
  //  Path[] localPath = {
  //    new Path("/tmp/initialNodeRank"), 
  //    new Path("/tmp/initialNodeRank.map2idx")
  //  };
  //  FileOutputStream file = new FileOutputStream(localPath[0].toString());
  //  DataOutputStream out = new DataOutputStream(file);
  //  BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
  //  FileOutputStream idxFile = new FileOutputStream(localPath[1].toString());
  //  DataOutputStream idxOut = new DataOutputStream(idxFile);
  //  int prevOff = 0;
  //  int currOff = 0;
  //  System.out.println("generating initial rank vector");
  // // for (int i = 0; i < numNodes; i += blockSize) {
  // //   //in each block write a block and an index
  // //   int max = i + blockSize;
  // //   String blockId = "node" + "\t" + i / blockSize;
  // //   StringBuilder sb = new StringBuilder();
  // //   for (int j = i; j < max; ++j) {
  // //     sb.append("" + j + "\t" + 1 / (float)numNodes + "\n");
  // //   }
  // //   out.writeBytes(sb.toString());
  // //   currOff += sb.toString().length();
  // //   idxOut.write(IndexingConstants.IDX_START);
  // //   Text.writeString(idxOut, blockId);
  // //   idxOut.write(IndexingConstants.SEG_START);
  // //   idxOut.writeLong(prevOff);
  // //   idxOut.writeLong(currOff - prevOff);
  // //   idxOut.write(IndexingConstants.IDX_END);
  // //   prevOff = currOff;
  // //   System.out.print(".");
  // // }
  // // System.out.print("\n");

  //  for (int i = 0; i < numNodes / blockSize; ++i) {
  //    String blockId = "node" + "\t" + i;
  //    StringBuilder sb = new StringBuilder();
  //    for (int j = i; j < numNodes; j += numNodes / blockSize) {
  //      sb.append("" + j + "\t" + 1 / (float)numNodes + "\n");
  //    }
  //    writer.write(sb.toString());
  //    currOff += sb.toString().length();
  //    idxOut.write(IndexingConstants.IDX_START);
  //    Text.writeString(idxOut, blockId);
  //    idxOut.write(IndexingConstants.SEG_START);
  //    idxOut.writeLong(prevOff);
  //    idxOut.writeLong(currOff - prevOff);
  //    idxOut.write(IndexingConstants.IDX_END);
  //    prevOff = currOff;
  //    System.out.print(".");
  //  }
  //  System.out.print("\n");
  //  writer.flush();
  //  out.close();
  //  idxOut.close();

  //  //copy to hdfs
  //  FileSystem fs = FileSystem.get(conf);
  //  fs.mkdirs(outPath);
  //  fs.copyFromLocalFile(false, true, localPath, outPath);
  //}
}
