
package bench.pagerank;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class PagerankBlock extends Configured implements Tool {
    protected static enum PrCounters { CONVERGE_CHECK }
    protected static double converge_threshold = 0.000001;

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

			if( line.length < 2 ) return;

			if( line.length == 2 ) {	// vector. 
        if (Integer.parseInt(line[0]) == 114505) {
          context.setStatus("found 114505");
        }
				context.write(new IntWritable(Integer.parseInt(line[0])), 
                      new Text(line[1]));
      } 
      else {					// edge
				context.write(new IntWritable(Integer.parseInt(line[1])), 
                      new Text(line[0] + "\t" + line[2]));
			}
		}
	}


    public static class	RedStage1 
          extends Reducer<IntWritable, Text, IntWritable, Text> {

		protected int blockWidth;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
			blockWidth = conf.getInt("pagerank.block.width", 1);
		}

		public void reduce (final IntWritable key, final Iterable<Text> values, 
                        final Context context) 
        throws IOException, InterruptedException {

			int i;
			float vector_val = 0;

			ArrayList<Integer> to_nodes_list = new ArrayList<Integer>();
			ArrayList<Float> to_val_list = new ArrayList<Float>();

			ArrayList<VectorElem<Double>> vectorArr = null;		// save vector
			ArrayList<ArrayList<BlockElem<Double>>> blockArr = new ArrayList<ArrayList<BlockElem<Double>>>();	// save blocks
			ArrayList<Integer> blockRowArr = new ArrayList<Integer>();	// save block rows(integer)

      for (Text val: values) {
        // vector: key=BLOCKID, value= (IN-BLOCK-INDEX VALUE)s
        // matrix: key=BLOCK-COL	BLOCK-ROW, value=(IN-BLOCK-COL IN-BLOCK-ROW VALUE)s
				String line_text = val.toString();
				final String[] line = line_text.split("\t");

				if( line.length == 1 ) {	// vector : VALUE
					vectorArr = GIMV.parseVectorVal(line_text.substring(1), Double.class);
				} else {					// edge : ROWID		VALUE
					blockArr.add( GIMV.parseBlockVal(line[1], Double.class) );	
					int block_row = Integer.parseInt(line[0]);
					blockRowArr.add( block_row );
				}
			}

			// output 'self' block to check convergence

			Text self_output = GIMV.formatVectorElemOutput("s", vectorArr);
			context.write(key, self_output );

			int blockCount = blockArr.size();
			if( vectorArr == null || blockCount == 0 ) {// missing vector or block.
				return;
      }

			// For every matrix block, join it with vector and output partial results
			Iterator<ArrayList<BlockElem<Double>>> blockArrIter = blockArr.iterator();
			Iterator<Integer> blockRowIter = blockRowArr.iterator();
			while( blockArrIter.hasNext() ){
				ArrayList<BlockElem<Double>> cur_block = blockArrIter.next();
				int cur_block_row = blockRowIter.next();

				// multiply cur_block and vectorArr. 
				ArrayList<VectorElem<Double>> cur_mult_result = GIMV.multBlockVector( cur_block, vectorArr, blockWidth);
				StringBuilder cur_block_output = new StringBuilder("o");
				if( cur_mult_result != null && cur_mult_result.size() > 0 ) {
					Iterator<VectorElem<Double>> cur_mult_result_iter = cur_mult_result.iterator();

					while( cur_mult_result_iter.hasNext() ) {
						VectorElem elem = cur_mult_result_iter.next();
						if( cur_block_output.length() != 1)
							cur_block_output.append(" ");
						cur_block_output.append("" + elem.row + " " + elem.val);
					}
					
					// output the partial result of multiplication.
					context.write(new IntWritable(cur_block_row), 
                        new Text(cur_block_output.toString()));
        }
      }
    }

    }


    //////////////////////////////////////////////////////////////////////
    // PASS 2: merge partial multiplication results
    //////////////////////////////////////////////////////////////////////
	public static class MapStage2 
        extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map (final LongWritable key, final Text value, 
                     final Context context) 
        throws IOException, InterruptedException {
			final String[] line = value.toString().split("\t");

			IntWritable node_key = new IntWritable(Integer.parseInt(line[0]));
			context.write(node_key, new Text(line[1]) );
		}
  }

    public static class RedStage2 
          extends Reducer<IntWritable, Text, IntWritable, Text> {
		int blockWidth;
		double alpha = 0;
		double threshold = 0;
		int numNodes = 1;
    boolean reportedChange = false;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", 1);
			blockWidth = conf.getInt("pagerank.block.width", 1);
      alpha = (double) conf.getFloat("pagerank.alpha", 0.15f);
      threshold = (double) conf.getFloat("pagerank.converge.threshold", 
                                         0.0001f);
		}

		public void reduce (final IntWritable key, final Iterable<Text> values, 
                        final Context context) 
        throws IOException, InterruptedException {
			ArrayList<VectorElem<Double>> self_vector = null;
			int i;
			double [] out_vals = new double[blockWidth];

			for(i=0; i < blockWidth; i++)
				out_vals[i] = 0;

      for (Text val : values) {
				String cur_str = val.toString();

				if( cur_str.charAt(0) == 's' ) {
					self_vector = GIMV.parseVectorVal(cur_str.substring(1), Double.class);
					continue;
				}
				ArrayList<VectorElem<Double>> cur_vector = GIMV.parseVectorVal(cur_str.substring(1), Double.class);
        if (cur_vector == null) {
          throw new IOException("cur_vector null");
        }

				Iterator<VectorElem<Double>> vector_iter = cur_vector.iterator();

				while( vector_iter.hasNext() ) {
					VectorElem<Double> v_elem = vector_iter.next();
					out_vals[ v_elem.row ] += v_elem.val;
				}
			}

			// output updated PageRank
			StringBuilder out_str = new StringBuilder("v");
			for(i = 0; i < blockWidth; i++) {
				if( out_str.length() >1 )
					out_str.append(" ");
				out_vals[i] = out_vals[i] * (1 - alpha) + alpha / numNodes;
				out_str.append("" + i + " " + out_vals[i]) ;
			}

			context.write( key, new Text(out_str.toString()) );

			// compare the previous and the current PageRank

			Iterator<VectorElem<Double>> sv_iter = self_vector.iterator();

			while( sv_iter.hasNext() && !reportedChange ) {
				VectorElem<Double> cur_ve = sv_iter.next();
			
				double diff = Math.abs(cur_ve.val - out_vals[cur_ve.row]);

				if( diff > threshold ) {
          context.getCounter(PrCounters.CONVERGE_CHECK).increment(1);
          reportedChange = true;
          break;
        }
      }
    }
    }

    //////////////////////////////////////////////////////////////////////
    // PASS 2.5: unfold the converged block PageRank results to plain format.
	//         This is a map-only stage.
	//  - Input: the converged block PageRank vector
	//  - Output: (node_id, "v"PageRank_of_the_node)
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage25 extends 
        Mapper<LongWritable, Text, IntWritable, Text> {

		int blockWidth;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      blockWidth = conf.getInt("pagerank.block.width", 1);
		}

		// input sample :
		//0       v0 0.11537637712698735 1 0.11537637712698735
		public void map (final LongWritable key, final Text value, 
                     Context context) 
        throws IOException, InterruptedException {
			final String[] line = value.toString().split("\t");
			final String[] tokens = line[1].substring(1).split(" ");
			int i;
			int block_id = Integer.parseInt(line[0] );


			for(i = 0; i < tokens.length; i+=2) {
				int elem_row = Integer.parseInt(tokens[i]);
				double pagerank = Double.parseDouble(tokens[i+1]);

				context.write( new IntWritable(blockWidth * block_id + elem_row), new Text("v" + pagerank) );
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edgePath = null;
    protected Path blkEdgePath = null;
    protected Path initNodePath = null;
    protected Path nodePath = null;
    protected Path initPath = null;
    protected Path tmpPath = null;
    protected Path outPath = null;
    protected Configuration conf;

    public static void main(final String[] args) {
      try {
        final int result = ToolRunner.run(new Configuration(), 
                                          new PagerankBlock(),
                                          args);
        System.exit(result);
      }
      catch (Exception e) {
        System.out.println("Exception: " + StringUtils.stringifyException(e));
        System.exit(-1);
      }
    }

    protected static int printUsage () {
      System.out.println("PagerankBlock <edgePath> <outPath>");

      return -1;
    }

    public int run (final String[] args) throws Exception {
      if( args.length != 2 ) {
        return printUsage();
      }
      conf = getConf();
      conf.addResource("pagerank-conf.xml");
      checkValidity();

      edgePath = new Path(args[0]);
      outPath = new Path(args[1]);
      FileSystem fs = FileSystem.get(conf);
      long start, end;

      blkEdgePath = new Path(edgePath.getParent(), "blkedge");
      initNodePath = new Path(edgePath.getParent(), "initialNodeRank");
      nodePath = new Path(edgePath.getParent(), "node");
      tmpPath = new Path(edgePath.getParent(), "tmp");

      PagerankBlockPrep prepare = new PagerankBlockPrep();
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
        System.out.println("===block experiment===<time>[PagerankBlockPrep]: " + 
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
          System.out.println("===block experiment===<time>[PagerankBlockPrep]: " + 
                             (end - start) + " ms");
        }
      }

      fs.delete(nodePath, true);
      fs.delete(outPath, true);
      int maxNumIterations = conf.getInt("pagerank.max.num.iteration", 100);
      System.out.println("Start iterating");
      boolean converged = false;
      start = System.currentTimeMillis();
      for (int i = 0; i < maxNumIterations; ++i) {
        long iterStart = System.currentTimeMillis();
        Job job;
        if (i == 0) {
          //first iteration read from initNodePath
          waitForJobFinish(configStage0());
        }
        else {
          //Every iteration we read from edgePath and nodePath and output to
          //outPath.
          waitForJobFinish(configStage1());
        }
        job = waitForJobFinish(configStage2());
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
        fs.delete(nodePath);
        fs.delete(tmpPath);
        fs.rename(outPath, nodePath);
        long iterEnd = System.currentTimeMillis();
        System.out.println("===map2 experiment===<iter time>" + 
                           "[PagerankMap2Iterative]: " + 
                           (iterEnd - iterStart) + " ms");
      }

      //put outPath into nodePath
      waitForJobFinish(configStage25());
      fs.delete(outPath);
      fs.rename(nodePath, outPath);

      return 0;
    }

    private void checkValidity() {
      int blockWidth = conf.getInt("pagerank.block.width", -1);
      if (blockWidth == -1) 
        throw new IllegalArgumentException("block width not set");
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

  protected Job configStage0() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankBlockStage0");
    job.setJarByClass(PagerankBlock.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, blkEdgePath, initNodePath);  
		FileOutputFormat.setOutputPath(job, tmpPath);  
		return job;
  }

  // Configure pass1
  protected Job configStage1() throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankBlockStage1");
    job.setJarByClass(PagerankBlock.class);
    job.setMapperClass(MapStage1.class);
    job.setReducerClass(RedStage1.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, blkEdgePath, nodePath);  
		FileOutputFormat.setOutputPath(job, tmpPath);  
		return job;
  }


  // Configure pass2
  protected Job configStage2 () throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankBlockStage2");
    job.setJarByClass(PagerankBlock.class);
    job.setMapperClass(MapStage2.class);
    job.setReducerClass(RedStage2.class);
    job.setNumReduceTasks(numReducers);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, tmpPath);  
    FileOutputFormat.setOutputPath(job, outPath);  
		return job;
  }

  // Configure pass25
  protected Job configStage25 () throws Exception {
    int numReducers = conf.getInt("pagerank.num.reducers", 1);
    Job job = new Job(conf, "PagerankBlockStage2");
    job.setJarByClass(PagerankBlock.class);
    job.setMapperClass(MapStage25.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, outPath);  
    FileOutputFormat.setOutputPath(job, nodePath);  
		return job;
  }
}

