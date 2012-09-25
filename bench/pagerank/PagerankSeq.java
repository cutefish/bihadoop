
/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: PageRankNaive.java
 - PageRank using plain matrix-vector multiplication.
Version: 2.0
***********************************************************************/

package bench.pagerank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class PagerankSeq extends Configured implements Tool {
  protected static enum PrCounters { CONVERGE_CHECK }
	protected static double threshold;

  //////////////////////////////////////////////////////////////////////
  // STAGE 1: Generate partial matrix-vector multiplication results.
	//          Perform hash join using Vector.rowid == Matrix.colid.
	//  - Input: edge_file, pagerank vector
	//  - Output: partial matrix-vector multiplication results.
  //////////////////////////////////////////////////////////////////////
	public static class MapStage1 
        extends Mapper<BytesWritable, BytesWritable, 
                        BytesWritable, BytesWritable> {

		public void map (final BytesWritable key, final BytesWritable value, 
                     final Context context) 
        throws IOException, InterruptedException {

      long start, end;
      start = System.currentTimeMillis();
      if (key.getLength() == 1) {
        //node block, write an extra byte after key for secondary sorting
        ByteBuffer keyBuf = ByteBuffer.wrap(key.getBytes());
        int blockRowId = keyBuf.getInt();
        ByteBuffer newKeyBuf = ByteBuffer.allocate(8);
        newKeyBuf.putInt(blockRowId);
        newKeyBuf.putInt(-1);
        context.write(new BytesWritable(newKeyBuf.array()), value);
      }
      else {
        if (key.getLength() != 2) {
          throw new IOException("key size not correct: " + key.getLength());
        }
        //edge block, write colId, rowId as key
        ByteBuffer keyBuf = ByteBuffer.wrap(key.getBytes());
        int blockRowId = keyBuf.getInt();
        int blockColId = keyBuf.getInt();
        ByteBuffer newKeyBuf = ByteBuffer.allocate(8);
        newKeyBuf.putInt(blockColId);
        newKeyBuf.putInt(blockRowId);
        context.write(new BytesWritable(newKeyBuf.array()), value);
      }
      end = System.currentTimeMillis();
      System.out.println("map1 time: " + (end - start) + " ms");
		}
	}

  public static class RedStage1 
        extends Reducer<BytesWritable, BytesWritable, 
                        BytesWritable, BytesWritable> {

    int numRowBlocks;
    int numColBlocks;
    int numNodes;

    public void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", 1);
      numRowBlocks = conf.getInt("pagerank.num.row.blocks", -1);
      numColBlocks = conf.getInt("pagerank.num.col.blocks", -1);

      System.out.println("num nodes: " + numNodes);
      System.out.println("num row blocks: " + numRowBlocks);
      System.out.println("num col blocks: " + numColBlocks);
    }

		public void reduce (final BytesWritable key, 
                        final Iterable<BytesWritable> values, 
                        final Context context) 
        throws IOException, InterruptedException {

      ByteBuffer keyBuf = ByteBuffer.wrap(key.getBytes());
      int blockColId = keyBuf.getInt();
      int nodePadding = keyBuf.getInt();
      if (nodePadding != -1) {
        throw new IOException("node padding incorrect: " + nodePadding);
      }

      int left = numNodes - numColBlocks * (numNodes / numColBlocks);
      int colBlockSize = numNodes / numColBlocks + 
          ((blockColId < left) ? 1 : 0);

      double prevRank[] = new double[colBlockSize];
      double currProb[] = new double[numNodes];

      long start, end;

      start = System.currentTimeMillis();
      boolean first = true;
      for (BytesWritable value : values) {
        int numBytes = value.getLength();
        if (first) {
          //first is ganranteed to be node previous rank
          ByteBuffer valBuf = ByteBuffer.wrap(value.getBytes());
          while(numBytes > 0) {
            int id = valBuf.getInt();
            double rank = valBuf.getDouble();
            int idInBlock = id / numColBlocks;
            prevRank[idInBlock] = rank;
            numBytes -= (4 + 8);
          }
          first = false;
        }
        else {
          //edge xferProb
          ByteBuffer valBuf = ByteBuffer.wrap(value.getBytes());
          while(numBytes > 0) {
            int rowId = valBuf.getInt();
            int colId = valBuf.getInt();
            double rank = valBuf.getDouble();
            int idInBlock = colId / numColBlocks;
            double xferProb = prevRank[idInBlock] * rank;
            currProb[rowId] += xferProb;
            numBytes -= (4 + 4 + 8);
          }
        }
			}
      end = System.currentTimeMillis();
      System.out.println("reduce1 read time: " + (end - start) + " ms");

      context.setStatus("writing output");

      start = System.currentTimeMillis();
      //emit both prevRank and currProb
      ByteBuffer emitKeyBuf, emitValBuf;
      //prevRank, add -1 for key
      emitKeyBuf = ByteBuffer.allocate(8);
      emitKeyBuf.putInt(blockColId);
      emitKeyBuf.putInt(-1);
      emitValBuf = ByteBuffer.allocate((4 + 8) * colBlockSize);
      int n = 0;
      for (int i = blockColId; i < numNodes; i += numColBlocks) {
        emitValBuf.putInt(i);
        emitValBuf.putDouble(prevRank[n]);
        n++;
      }
      context.write(new BytesWritable(emitKeyBuf.array()),
                    new BytesWritable(emitValBuf.array()));
      //currProb, add 0 for key
      emitKeyBuf = ByteBuffer.allocate(8);
      emitKeyBuf.putInt(blockColId);
      emitKeyBuf.putInt(0);
      for (int i = 0; i < numColBlocks; i++) {
        int blockSize = numNodes / numColBlocks + 
            ((i < left) ? 1 : 0);
        emitValBuf = ByteBuffer.allocate((4 + 8) * blockSize);
        for (int j = i; j < numNodes; j += numColBlocks) {
          emitValBuf.putInt(j);
          emitValBuf.putDouble(currProb[j]);
        }
        context.write(new BytesWritable(emitKeyBuf.array()),
                      new BytesWritable(emitValBuf.array()));
      }
      end = System.currentTimeMillis();
      System.out.println("reduce1 write time: " + (end - start) + " ms");
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // STAGE 2: merge multiplication results.
  //  - Input: partial multiplication results
  //  - Output: combined multiplication results
  //////////////////////////////////////////////////////////////////////////////
  public static class MapStage2 
        extends Mapper<BytesWritable, BytesWritable, 
                        BytesWritable, BytesWritable> {
		// Identity mapper
    public void map (final BytesWritable key, 
                     final BytesWritable value, 
                     final Context context) 
        throws IOException, InterruptedException {

      long start, end;
      start = System.currentTimeMillis();
			context.write(key, value);
      end = System.currentTimeMillis();
      System.out.println("map2 time: " + (end - start) + " ms");
		}
	}

  public static class RedStage2 
        extends Reducer<BytesWritable, BytesWritable, 
                        BytesWritable, BytesWritable> {

		int numNodes = 0;
    int numColBlocks = 0;
		double alpha = 0;
		double threshold = 0;
    boolean reportedChange = false;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", 1);
      numColBlocks = conf.getInt("pagerank.num.col.blocks", 1);
      alpha = (double) conf.getFloat("pagerank.alpha", 0.15f);
      threshold = (double) conf.getFloat("pagerank.converge.threshold", 
                                         0.0001f);
		}

    public void reduce(final BytesWritable key, 
                       final Iterable<BytesWritable> values, 
                       final Context context) 
        throws IOException, InterruptedException {

      ByteBuffer keyBuf = ByteBuffer.wrap(key.getBytes());
      int blockColId = keyBuf.getInt();
      int nodePadding = keyBuf.getInt();
      if (nodePadding != -1) {
        throw new IOException("node padding incorrect: " + nodePadding);
      }

      int left = numNodes - numColBlocks * (numNodes / numColBlocks);
      int colBlockSize = numNodes / numColBlocks + 
          ((blockColId < left) ? 1 : 0);

			double prevRank[] = new double[colBlockSize];
			double currRank[] = new double[colBlockSize];

      long start, end;

      start = System.currentTimeMillis();
      boolean first = true;
      for (BytesWritable value : values) {
        int numBytes = value.getLength();
        if (first) {
          //first is ganranteed to be node previous rank
          ByteBuffer valBuf = ByteBuffer.wrap(value.getBytes());
          while(numBytes > 0) {
            int id = valBuf.getInt();
            double rank = valBuf.getDouble();
            int idInBlock = id / numColBlocks;
            prevRank[idInBlock] = rank;
            numBytes -= (4 + 8);
          }
          first = false;
        }
        else {
          //node current rank
          ByteBuffer valBuf = ByteBuffer.wrap(value.getBytes());
          while(numBytes > 0) {
            int id = valBuf.getInt();
            double rank = valBuf.getDouble();
            int idInBlock = id / numColBlocks;
            currRank[idInBlock] += rank;
            numBytes -= (4 + 8);
          }
        }
			}

      // add random jump and compare with prev
      for (int i = 0; i < colBlockSize; ++i) {
        currRank[i] = currRank[i] * (1 - alpha) + alpha / numNodes;
        if (!reportedChange) {
          double diff = Math.abs(prevRank[i] - currRank[i]);
          if( diff > threshold ) {
            context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
            reportedChange = true;
          }
        }
      }
      end = System.currentTimeMillis();
      System.out.println("reduce2 read time: " + (end - start) + " ms");

      start = System.currentTimeMillis();
      //emit result
      ByteBuffer emitKeyBuf, emitValBuf;
      emitKeyBuf = ByteBuffer.allocate(4);
      emitKeyBuf.putInt(blockColId);
      emitValBuf = ByteBuffer.allocate((4 + 8) * colBlockSize);
      int n = 0;
      for (int i = blockColId; i < numNodes; i += numColBlocks) {
        emitValBuf.putInt(i);
        emitValBuf.putDouble(currRank[n]);
        n++;
      }
      context.write(new BytesWritable(emitKeyBuf.array()),
                    new BytesWritable(emitValBuf.array()));
      end = System.currentTimeMillis();
      System.out.println("reduce1 write time: " + (end - start) + " ms");
    }
  }

  public static class PrSeqSortComparator 
        extends WritableComparator {

    protected PrSeqSortComparator() {
      super(BytesWritable.class, true);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      ByteBuffer bbuf;
      bbuf = ByteBuffer.wrap(b1);
      int colId1 = bbuf.getInt();
      int rowId1 = bbuf.getInt();
      bbuf = ByteBuffer.wrap(b1);
      int colId2 = bbuf.getInt();
      int rowId2 = bbuf.getInt();
      if (colId1 != colId2) {
        return colId1 - colId2;
      }
      return rowId1 - rowId2;
    }
  }

  public static class PrSeqGroupComparator 
        extends WritableComparator {

    protected PrSeqGroupComparator() {
      super(BytesWritable.class, true);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      ByteBuffer bbuf;
      bbuf = ByteBuffer.wrap(b1);
      int colId1 = bbuf.getInt();
      bbuf = ByteBuffer.wrap(b1);
      int colId2 = bbuf.getInt();
      return colId1 - colId2;
    }
  }

  /****************************************************************************
   * Stage 2.5 convert back to plain txt
   ***************************************************************************/
  public static class MapStage25 
        extends Mapper<BytesWritable, BytesWritable, 
                        Text, Text> {
		// Identity mapper
    public void map (final BytesWritable key, 
                     final BytesWritable value, 
                     final Context context) 
        throws IOException, InterruptedException {

      long start, end;
      start = System.currentTimeMillis();
      ByteBuffer buf = ByteBuffer.wrap(value.getBytes());
      int size = value.getLength() / (4 + 8);
      for (int i = 0; i < size; ++i) {
        int rowId = buf.getInt();
        double rank = buf.getDouble();
        context.write(new Text("" + rowId),
                      new Text("" + rank));
      }
      end = System.currentTimeMillis();
      System.out.println("map2 time: " + (end - start) + " ms");
		}
	}

  /****************************************************************************
   * command line
   ***************************************************************************/

  protected Path edgePath = null;
  protected Path blkEdgePath = null;
  protected Path initNodePath = null;
  protected Path tmpPath = null;
  protected Path nodePath = null;
  protected Path outPath = null;
  protected Configuration conf = null;

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new PagerankSeq(),
                                        args);
      System.out.println("PagerankSeq main return: " + result);
      return;
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankSeq <edgePath> <outPath>");
    return -1;
  }

  public int run (final String[] args) throws Exception
  {
    if( args.length != 2 ) {
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
    blkEdgePath = new Path(edgePath.getParent(), "blkedge");
		initNodePath = new Path(edgePath.getParent(), "initialNodeRank");
    nodePath = new Path(edgePath.getParent(), "node");
    tmpPath = new Path(edgePath.getParent(), "tmp");
    System.out.println(edgePath);
    System.out.println(initNodePath);
    System.out.println(outPath);
    System.out.println(nodePath);
    System.out.println(tmpPath);
		final FileSystem fs = FileSystem.get(conf);
    long start, end;

    PagerankSeqPrep prepare = new PagerankSeqPrep();
    prepare.setPaths(edgePath.toString(), blkEdgePath.toString(),
                     initNodePath.toString());

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
      System.out.println("===map2 experiment===<time>[PagerankMap2Prep]: " + 
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
        System.out.println("===map2 experiment===<time>[PagerankMap2Prep]: " + 
                           (end - start) + " ms");
      }
    }
    fs.delete(nodePath);
    fs.delete(outPath);
    fs.delete(tmpPath);
    int maxNumIterations = conf.getInt("pagerank.max.num.iteration", 100);

    System.out.println("Start iterating");
    boolean converged = false;
    start = System.currentTimeMillis();
		for (int i = 0; i < maxNumIterations; ++i) {
      long iterStart = System.currentTimeMillis();
      Job job;

      //first iteration
      if (i == 0) {
        waitForJobFinish(configStage0());
        job = waitForJobFinish(configStage2());
      }
      else {
        waitForJobFinish(configStage1());
        job = waitForJobFinish(configStage2());
      }

			// The counter is newly created per every iteration.
			Counters c = job.getCounters();
			long changed = c.findCounter(PrCounters.CONVERGE_CHECK).getValue();
      System.out.println("Iteration: " + i + " changed: " + changed);
      if (changed == 0) {
        System.out.println("Converged.");
        fs.delete(tmpPath);
        fs.delete(nodePath);
        converged = true;
        break;
      }
			// rotate directory
			fs.delete(tmpPath);
			fs.delete(nodePath);
			fs.rename(outPath, nodePath);
      long iterEnd = System.currentTimeMillis();
      System.out.println("===map2 experiment===<iter time>[PagerankNaiveIterative]: " + 
                         (iterEnd - iterStart) + " ms");
		}
    end = System.currentTimeMillis();
    System.out.println("===map2 experiment===<time>[PagerankNaiveIterative]: " + 
                       (end - start) + " ms");

    if (!converged) {
      System.out.println("Reached the max iteration.");
      fs.rename(nodePath, outPath);
    }
    if (conf.getBoolean("pagerank.keep.intermediate", false)) {
      FileUtil.copy(fs, outPath, fs, nodePath, false, true, conf);
    }
    if (conf.getBoolean("pagerank.map2.toplaintxt", false)) {
      waitForJobFinish(configStage25());
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

  // Configure pass0
  // input on initial node
  protected Job configStage0() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankSeqStage0");
    job.setJarByClass(PagerankSeq.class);

    job.setMapperClass(MapStage1.class);
		job.setReducerClass(RedStage1.class);
    job.setSortComparatorClass(PrSeqSortComparator.class);
    job.setGroupingComparatorClass(PrSeqGroupComparator.class);

    job.setNumReduceTasks(numReducers);

    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
		FileInputFormat.setInputPaths(job, edgePath, initNodePath);  
		FileOutputFormat.setOutputPath(job, tmpPath);  
		return job;
  }

  // Configure pass1
  protected Job configStage1() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankSeqStage1");
    job.setJarByClass(PagerankSeq.class);

    job.setMapperClass(MapStage1.class);
		job.setReducerClass(RedStage1.class);
    job.setSortComparatorClass(PrSeqSortComparator.class);
    job.setGroupingComparatorClass(PrSeqGroupComparator.class);

    job.setNumReduceTasks(numReducers);

    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
		FileInputFormat.setInputPaths(job, edgePath, nodePath);  
		FileOutputFormat.setOutputPath(job, tmpPath);  
		return job;
  }

  // Configure pass2
  protected Job configStage2 () throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankSeqStage2");
    job.setJarByClass(PagerankSeq.class);

    job.setMapperClass(MapStage2.class);
		job.setReducerClass(RedStage2.class);
    job.setSortComparatorClass(PrSeqSortComparator.class);
    job.setGroupingComparatorClass(PrSeqGroupComparator.class);

    job.setNumReduceTasks(numReducers);

    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
		FileInputFormat.setInputPaths(job, tmpPath);  
		FileOutputFormat.setOutputPath(job, outPath);  
		return job;
  }

  //configure pass 2.5
  protected Job configStage25 () throws Exception {
    Path plainTextPath = new Path(outPath.getParent(), 
                                  "plainText");
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankSeqStage25");
    job.setJarByClass(PagerankSeq.class);

    job.setMapperClass(MapStage25.class);

    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
		FileInputFormat.setInputPaths(job, outPath);  
		FileOutputFormat.setOutputPath(job, plainTextPath);  
		return job;
  }

}

