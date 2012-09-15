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

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class PagerankNaive extends Configured implements Tool {
  protected static enum PrCounters { CONVERGE_CHECK }
	protected static double threshold;

  //////////////////////////////////////////////////////////////////////
  // STAGE 1: Generate partial matrix-vector multiplication results.
	//          Perform hash join using Vector.rowid == Matrix.colid.
	//  - Input: edge_file, pagerank vector
	//  - Output: partial matrix-vector multiplication results.
  //////////////////////////////////////////////////////////////////////
	public static class MapStage1 
        extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map (final LongWritable key, final Text value, 
                     final Context context) 
        throws IOException, InterruptedException {

      String lineText = value.toString();
			if (lineText.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = lineText.split("\t");
			if(line.length < 2 ) return;

      //dealing with vectors
      //rowId + "\t" + pageRank + "\t" + "vec"
      if (line.length == 3 && line[2].equals("vec")) {
        context.write(new IntWritable(Integer.parseInt(line[0])),
                      new Text(line[1] + "\t" + "vec"));
        return;
      }

      if (line.length != 2) return;
      //dealing with adjacency matrix
      //srcId + "\t" + dstId
      //dstProb = srcDstXferProb * srcProb
      //so we should group srcId together
      int srcId = Integer.parseInt(line[0]);
      int dstId = Integer.parseInt(line[1]);
      context.write(new IntWritable(srcId), new Text(Integer.toString(dstId)));
		}
	}

  public static class RedStage1 
        extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce (final IntWritable key, 
                        final Iterable<Text> values, 
                        final Context context) 
        throws IOException, InterruptedException {

			double currRank = 0;

      ArrayList<Integer> dstNodes = new ArrayList<Integer>();

      for (Text value : values) {
				String lineText = value.toString();
				final String[] line = lineText.split("\t");

        if (line.length == 2 && lineText.endsWith("vec")) {
          currRank = Double.parseDouble(line[0]);
          continue;
        }

        dstNodes.add(Integer.parseInt(line[0]));
			}

			// emit currRank for comparision
			context.write(key, new Text("" + currRank + "\tprev"));

      int outDeg = dstNodes.size();
      if (outDeg > 0)
        currRank = currRank / (double)outDeg;

			for (int i = 0; i < outDeg; ++i) {
				context.write(new IntWritable(dstNodes.get(i)), 
                      new Text("" + currRank));
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // STAGE 2: merge multiplication results.
  //  - Input: partial multiplication results
  //  - Output: combined multiplication results
  //////////////////////////////////////////////////////////////////////////////
  public static class MapStage2 
        extends Mapper<LongWritable, Text, IntWritable, Text> {
		// Identity mapper
    public void map (final LongWritable key, 
                     final Text value, final Context context) 
        throws IOException, InterruptedException {
			final String[] line = value.toString().split("\t", 2);

			context.write(new IntWritable(Integer.parseInt(line[0])), 
                    new Text(line[1]));
		}
	}

  public static class RedStage2 
        extends Reducer<IntWritable, Text, IntWritable, Text> {

		int numNodes = 0;
		double alpha = 0;
		double threshold = 0;
    boolean reportedChange = false;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", 1);
      alpha = (double) conf.getFloat("pagerank.alpha", 0.15f);
      threshold = (double) conf.getFloat("pagerank.converge.threshold", 
                                         0.0001f);
		}

    public void reduce(final IntWritable key, final Iterable<Text> values, 
                       final Context context) 
        throws IOException, InterruptedException {

			double prevRank = 0;
			double currRank = 0;

      for (Text val : values) {
				String valText = val.toString();
        String[] value = valText.split("\t");
        if (value.length == 2 && valText.endsWith("prev")) {
          prevRank = Double.parseDouble(value[0]);
          continue;
        }
        currRank += Double.parseDouble(value[0] ) ;
			}

      currRank = currRank * (1 - alpha) + alpha / numNodes;
			context.write(key, new Text("" + currRank + "\tvec"));

			if (!reportedChange) {
				double diff = Math.abs(prevRank - currRank);

				if( diff > threshold ) {
          context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
					reportedChange = true;
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////
  // command line interface
  //////////////////////////////////////////////////////////////////////
  protected Configuration conf;
  protected Path edgePath = null;
  protected Path initNodePath = null;
  protected Path nodePath = null;
  protected Path tmpPath = null;
  protected Path outPath = null;

  // Main entry point.
  public static void main (final String[] args) throws Exception
  {
    final int result = ToolRunner.run(new Configuration(), new PagerankNaive(), args);

    System.exit(result);
  }


  // Print the command-line usage text.
  protected static int printUsage ()
  {
    System.out.println("PagerankNaive <edgePath> <outPath>");
    return -1;
  }

  // submit the map/reduce job.
  public int run (final String[] args) throws Exception
  {
    if( args.length != 2 ) {
			return printUsage();
		}

    long start, end;

		edgePath = new Path(args[0]);
    outPath = new Path(args[1]);
		initNodePath = new Path(edgePath.getParent(), "initialNodeRank");
    nodePath = new Path(edgePath.getParent(), "node");
    tmpPath = new Path(edgePath.getParent(), "tmp");
    System.out.println(edgePath);
    System.out.println(initNodePath);
    System.out.println(outPath);
    System.out.println(nodePath);
    System.out.println(tmpPath);

    conf = getConf();
    conf.addResource("pagerank-conf.xml");
    checkValidity();

		final FileSystem fs = FileSystem.get(conf);
    fs.delete(outPath);
    fs.delete(tmpPath);

    if (conf.getBoolean("pagerank.initialize", true)) {
      fs.delete(initNodePath);
      genInitNodeRanks();
    }
    else {
      if (!fs.exists(initNodePath)) {
        genInitNodeRanks();
      }
    }

    int maxNumIterations = conf.getInt("pagerank.max.num.iteration", 100);
		// Run pagerank until converges. 
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
		return 1;
  }

	// generate initial pagerank vector
	public void genInitNodeRanks() throws IOException {
    int numNodes = conf.getInt("pagerank.num.nodes", 1);
    String localPath = "/tmp/initialNodeRank";
    FileOutputStream file = new FileOutputStream(localPath);
    DataOutputStream out = new DataOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    System.out.println("generating initial rank vector");
    for (int i = 0; i < numNodes; ++i) {
      String line = "" + i + "\t" + 1 / (float)numNodes + "\t" + "vec\n";
      writer.write(line);
      if (i % numNodes/100 == 0) System.out.print(".");
    }
    writer.flush();
    out.close();
    System.out.print("\n");
    //copy to hdfs
    FileSystem fs = FileSystem.get(conf);
    fs.copyFromLocalFile(false, true, new Path(localPath), 
                         initNodePath);
  }

  // Configure pass0
  // input on initial node
  protected Job configStage0() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankNaiveStage0");
    job.setJarByClass(PagerankNaive.class);
    job.setMapperClass(MapStage1.class);
		job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, edgePath, initNodePath);  
		FileOutputFormat.setOutputPath(job, tmpPath);  
		return job;
  }

  // Configure pass1
  protected Job configStage1() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankNaiveStage1");
    job.setJarByClass(PagerankNaive.class);
    job.setMapperClass(MapStage1.class);
		job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, edgePath, nodePath);  
		FileOutputFormat.setOutputPath(job, tmpPath);  
		return job;
  }

  // Configure pass2
  protected Job configStage2 () throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankNaiveStage2");
    job.setJarByClass(PagerankNaive.class);
    job.setMapperClass(MapStage2.class);
		job.setReducerClass(RedStage2.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, tmpPath);  
		FileOutputFormat.setOutputPath(job, outPath);  
		return job;
  }

  private void checkValidity() {
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
}

