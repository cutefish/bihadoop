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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	protected static double threshold = 0.000001;

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

      String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			if(line.length < 2 )
				return;

			if (line[1].charAt(0) == 'v') {	// vector : ROWID	VALUE('vNNNN')
				context.write(new IntWritable(Integer.parseInt(line[0])), 
                      new Text(line[1]));
			} 
      else {							
				// In other matrix-vector multiplication, we output (dst, src) here
				// However, In PageRank, the matrix-vector computation formula is M^T * v.
				// Therefore, we output (src,dst) here.
				int src_id = Integer.parseInt(line[0]);
				int dst_id = Integer.parseInt(line[1]);
				context.write(new IntWritable(src_id), new Text(line[1]));

				if (make_symmetric == 1)
					context.write(new IntWritable(dst_id), new Text(line[0]));
			}
		}
	}

  public static class RedStage1 
        extends Reducer<IntWritable, Text, IntWritable, Text> {

		int number_nodes = 0;
		double mixing_c = 0;
		double random_coeff = 0;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      number_nodes = conf.getInt("pagerank.num.nodes", -1);
      mixing_c = (double) conf.getFloat("pagerank.num.mixingc", 0.85f);
			random_coeff = (1-mixing_c) / (double)number_nodes;
		}

		public void reduce (final IntWritable key, 
                        final Iterable<Text> values, 
                        final Context context) 
        throws IOException, InterruptedException {
			int i;
			double cur_rank = 0;

      ArrayList<Integer> dst_nodes_list = new ArrayList<Integer>();

      for (Text value : values) {
				String line_text = value.toString();
				final String[] line = line_text.split("\t");

				if( line.length == 1 ) {	
					if(line_text.charAt(0) == 'v')	// vector : VALUE
						cur_rank = Double.parseDouble(line_text.substring(1));
					else {							// edge : ROWID
						dst_nodes_list.add(Integer.parseInt(line[0]));
					}
				} 
			}

			// add random coeff
			context.write(key, new Text( "s" + cur_rank));

      int outdeg = dst_nodes_list.size();
      if( outdeg > 0 )
        cur_rank = cur_rank / (double)outdeg;

			for( i = 0; i < outdeg; i++) {
				context.write(new IntWritable(dst_nodes_list.get(i)), 
                      new Text("v" + cur_rank));
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // STAGE 2: merge multiplication results.
  //  - Input: partial multiplication results
  //  - Output: combined multiplication results
  ////////////////////////////////////////////////////////////////////////////////////////////////
  public static class MapStage2 
        extends Mapper<LongWritable, Text, IntWritable, Text> {
		// Identity mapper
    public void map (final LongWritable key, 
                     final Text value, final Context context) 
        throws IOException, InterruptedException {
			final String[] line = value.toString().split("\t");

			context.write(new IntWritable(Integer.parseInt(line[0])), 
                    new Text(line[1]));
		}
	}

  public static class RedStage2 
        extends Reducer<IntWritable, Text, IntWritable, Text> {

		int number_nodes = 0;
		double mixing_c = 0;
		double random_coeff = 0;
		double converge_threshold = 0;
		int change_reported = 0;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      number_nodes = conf.getInt("pagerank.num.nodes", -1);
      mixing_c = (double) conf.getFloat("pagerank.num.mixingc", 0.85f);
			random_coeff = (1-mixing_c) / (double)number_nodes;
      converge_threshold = (double) conf.getFloat(
          "pagerank.converge.threshold", 0.000001f);
		}

    public void reduce(final IntWritable key, final Iterable<Text> values, 
                       final Context context) 
        throws IOException, InterruptedException {

			int i;
			double next_rank = 0;
			double previous_rank = 0;

      for (Text value : values) {
				String cur_value_str = value.toString();
				if( cur_value_str.charAt(0) == 's' )
					previous_rank = Double.parseDouble(cur_value_str.substring(1));
				else
					next_rank += Double.parseDouble( cur_value_str.substring(1) ) ;
			}

			next_rank = next_rank * mixing_c + random_coeff;

			context.write(key, new Text("v" + next_rank));


			if( change_reported == 0 ) {
				double diff = Math.abs(previous_rank-next_rank);

				if( diff > converge_threshold ) {
          context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
					change_reported = 1;
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////
  // command line interface
  //////////////////////////////////////////////////////////////////////
  protected Configuration conf = getConf();
  protected Path edge_path = null;
  protected Path vector_path = null;
  protected Path tempmv_path = null;
  protected Path output_path = null;
  protected String local_output_path;
  protected Path minmax_path = new Path("pr_minmax");
  protected Path distr_path = new Path("pr_distr");
  protected int number_nodes = 0;
  protected int niteration = 32;
  protected double mixing_c = 0.85f;
  protected int nreducers = 1;
  protected int make_symmetric = 0;		// convert directed graph to undirected graph

  // Main entry point.
  public static void main (final String[] args) throws Exception
  {
    final int result = ToolRunner.run(new Configuration(), new PagerankNaive(), args);

    System.exit(result);
  }


  // Print the command-line usage text.
  protected static int printUsage ()
  {
    System.out.println("PagerankNaive <edge_path> <temppr_path> <output_path>");

    ToolRunner.printGenericCommandUsage(System.out);

    return -1;
  }

  // submit the map/reduce job.
  public int run (final String[] args) throws Exception
  {
    if( args.length != 3 ) {
			return printUsage();
		}

		edge_path = new Path(args[0]);
		vector_path = new Path("pr_vector");
		tempmv_path = new Path(args[1]);
		output_path = new Path(args[2]);				

    conf.addResource("bench-common.xml");
    conf.addResource("pagerank-conf.xml");

		number_nodes = conf.getInt("pagerank.num.nodes", -1);
		nreducers = conf.getInt("pagerank.num.reducers", 1);
		niteration = conf.getInt("pagerank.num.iteration", -1);
		make_symmetric = conf.getInt("pagerank.symmetric", 0);
    String curIterFlag = conf.get("pagerank.curr.iteration");
		int cur_iteration = 1; 
    if (curIterFlag.startsWith("cont")) {
        cur_iteration = Integer.parseInt(curIterFlag);
    }
    checkValidity();

		local_output_path = output_path + "_temp";
		converge_threshold = ((double)1.0/(double) number_nodes)/10;
    conf.setFloat("pagerank.converge.threshold", (float) converge_threshold);

		System.out.println("\n===[PEGASUS: A Peta-Scale Graph Mining System]===\n");
		System.out.println("[PEGASUS] Computing PageRank. " + 
                       " Max iteration = " + niteration + 
                       " threshold = " + converge_threshold + 
                       " cur_iteration=" + cur_iteration + "\n");

		if( cur_iteration == 1 )
			gen_initial_vector(number_nodes, vector_path);

		final FileSystem fs = FileSystem.get(conf);

		// Run pagerank until converges. 
		for (int i = cur_iteration; i <= niteration; i++) {
      safeRunJob(configStage1(), "stage1");
      Job job = safeRunJob(configStage2(), "stage2");

			// The counter is newly created per every iteration.
			Counters c = job.getCounters();
			long changed = c.findCounter(PrCounters.CONVERGE_CHECK).getValue();
			System.out.println("Iteration = " + i + ", changed reducer = " + changed);

			if( changed == 0 ) {
				System.out.println("PageRank vector converged. Now preparing to finish...");
				fs.delete(vector_path);
				fs.delete(tempmv_path);
				fs.rename(output_path, vector_path);
				break;
			}

			// rotate directory
			fs.delete(vector_path);
			fs.delete(tempmv_path);
			fs.rename(output_path, vector_path);
      cur_iteration = i;
		}

		if( cur_iteration == niteration ) {
			System.out.println("Reached the max iteration. Now preparing to finish...");
		}

		// find min/max of pageranks
		System.out.println("Finding minimum and maximum pageranks...");
    safeRunJob(configStage3(), "stage3");

		FileUtil.fullyDelete(FileSystem.getLocal(conf), new Path(local_output_path));
		String new_path = local_output_path + "/" ;
		fs.copyToLocalFile(minmax_path, new Path(new_path) ) ;

		MinMaxInfo mmi = readMinMax(new_path);
		System.out.println("min = " + mmi.min + ", max = " + mmi.max );

		// find distribution of pageranks
    safeRunJob(configStage4(mmi.min, mmi.max), "stage4");

		System.out.println("\n[PEGASUS] PageRank computed.");
		System.out.println("[PEGASUS] The final PageRanks are in the HDFS pr_vector.");
		System.out.println("[PEGASUS] The minium and maximum PageRanks are in the HDFS pr_minmax.");
		System.out.println("[PEGASUS] The histogram of PageRanks in 1000 bins between min_PageRank and max_PageRank are in the HDFS pr_distr.\n");

		return 0;
  }

	// generate initial pagerank vector
	public void gen_initial_vector(int number_nodes, Path vector_path) 
      throws IOException {
		int i, j = 0;
		int milestone = number_nodes/10;
		String file_name = "pagerank_init_vector.temp";
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		System.out.print("Creating initial pagerank vectors...");
		double initial_rank = 1.0 / (double)number_nodes;

    for(i=0; i < number_nodes; i++) {
      out.write(i + "\tv" + initial_rank +"\n");
      if(++j > milestone) {
        System.out.print(".");
        j = 0;
      }
    }
    out.close();
    System.out.println("");

    // copy it to curbm_path, and delete temporary local file.
    final FileSystem fs = FileSystem.get(conf);
    fs.copyFromLocalFile(true, 
                         new Path("./" + file_name), 
                         new Path (vector_path.toString()+ "/" + file_name));
  }

  // Configure pass1
  protected Job configStage1 () throws Exception {
    Job job = new Job(conf, "PagerankNaiveStage1");
    job.setJarByClass(PagerankNaive.class);
    job.setMapperClass(MapStage1.class);
		job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(nreducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, edge_path, vector_path);  
		FileOutputFormat.setOutputPath(job, tempmv_path);  
		return job;
  }

  // Configure pass2
  protected Job configStage2 () throws Exception {
    Job job = new Job(conf, "PagerankNaiveStage2");
    job.setJarByClass(PagerankNaive.class);
    job.setMapperClass(MapStage2.class);
		job.setReducerClass(RedStage2.class);
    job.setNumReduceTasks(nreducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, tempmv_path);  
		FileOutputFormat.setOutputPath(job, output_path);  
		return job;
  }

  private void checkValidity() {
    if (number_nodes == -1) {
      System.out.println("error: number of nodes not set");
      System.exit(-1);
    }
  }

  private Job safeRunJob(Job job, String message) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      System.out.println("Job: " + message + " failed");
      System.exit(-1);
      }
    return job;
  }
}

