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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
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
public class PagerankSeqPrep extends Configured implements Tool {

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
        extends Reducer<Text, Text, BytesWritable, BytesWritable> {

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
      int rowId = Integer.parseInt(keyStrings[1]);
      int colId = Integer.parseInt(keyStrings[2]);
      ByteBuffer keyBuf = ByteBuffer.allocate(4 + 4);
      keyBuf.putInt(rowId);
      keyBuf.putInt(colId);
      context.write(new BytesWritable(keyBuf.array()), 
                    new BytesWritable(out.toByteArray()));
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
                                        new PagerankSeqPrep(),
                                        args);
      System.out.println("PagerankSeqPrep main return: " + result);
      return;
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankSeqPrep " + 
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
    if (conf == null) {
      conf = new Configuration();
    }
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    setPaths(args[0], args[1], args[2]);
    initNodes();
    blockEdges();

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

    SequenceFile.setCompressionType(conf, SequenceFile.CompressionType.NONE);
    SequenceFile.Writer out = SequenceFile.createWriter(
        fs, conf, blkNodePath, BytesWritable.class, BytesWritable.class);

    int left = numNodes - numColBlocks * (numNodes / numColBlocks);
    for (int i = 0; i < numColBlocks; ++i) {
      //put key into byte[]
      ByteBuffer keyBuf = ByteBuffer.allocate(4);
      keyBuf.putInt(i);
      //put value into byte[]
      int actualBlockSize = numNodes / numColBlocks + ((i < left) ? 1 : 0);
      int numBytes = actualBlockSize * (4 + 8);
      //allocate one int and one double for each element
      ByteBuffer valBuf = ByteBuffer.allocate(numBytes);
      for (int j = i; j < numNodes; j += numColBlocks) {
        valBuf.putInt(j);
        valBuf.putDouble(1.0 / numNodes);
      }
      SequenceFileAsBinaryOutputFormat.WritableValueBytes wvaluebytes = 
          new SequenceFileAsBinaryOutputFormat.WritableValueBytes();
      wvaluebytes.reset(new BytesWritable(valBuf.array()));
      out.appendRaw(keyBuf.array(), 0, 4, wvaluebytes);
      wvaluebytes.reset(null);
      //out.append(new BytesWritable(keyBuf.array()),
      //           new BytesWritable(valBuf.array()));
      System.out.print(".");
    }
    out.close();
    System.out.println("\n");
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
    Job job = new Job(conf, "PagerankSeqPrep");
    job.setJarByClass(PagerankSeqPrep.class);
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
    Job job = new Job(conf, "PagerankSeqPrepStage2");
    job.setJarByClass(PagerankSeqPrep.class);
    job.setMapperClass(MapStage2.class);
    job.setReducerClass(ReduceStage2.class);
    job.setNumReduceTasks(conf.getInt("pagerank.num.reducers", 1));
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
    job.setPartitionerClass(PagerankSeqPartitioner.class);
    FileInputFormat.setInputPaths(job, tmpPath);
    FileOutputFormat.setOutputPath(job, blkEdgePath);
    return job;
  }

  public static class PagerankSeqPartitioner<K, V>
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
