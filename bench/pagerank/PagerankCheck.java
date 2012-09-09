package bench.pagerank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

/**
 * A more concise way to check result of pagerank.
 * By showing min, max and histogram.
 */

public class PagerankCheck extends Configured implements Tool {

  /**
   * Find min, max.
   * Input: The converged PageRank vector
   * Output: (key 0) minimum PageRank, (key 1) maximum PageRank
   */
	public static class MapStage1 
        extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		public void map (final LongWritable key, final Text value, 
                     final Context context) 
        throws IOException, InterruptedException {

			String lineText = value.toString();
			if (lineText.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = lineText.split("\t");
			double pagerank = Double.parseDouble(line[1]);
			context.write(new IntWritable(0) , new DoubleWritable(pagerank));
			context.write(new IntWritable(1) , new DoubleWritable(pagerank));
		}
	}

  public static class RedStage1 
        extends Reducer<IntWritable, DoubleWritable, 
                        IntWritable, DoubleWritable> {

		public void reduce (final IntWritable key, 
                        final Iterable<DoubleWritable> values, 
                        final Context context) 
            throws IOException, InterruptedException {

			int i;
			double min = 1.0;
			double max = 0.0;

			int minOrMax = key.get();	// 0 : min, 1: max
      boolean findMin = (key.get() == 0);

      for (DoubleWritable value : values) {
				double curValue = value.get();

        if (findMin) {
          if (curValue < min)
            min = curValue;
				} 
        else {				// find max
					if (curValue > max )
            max = curValue;
				}
      }

			if (findMin)
				context.write(key, new DoubleWritable(min));
			else
				context.write(key, new DoubleWritable(max));
    }
  }

  /**
   * Find distribution of pageranks.
   * Input: pagerank vector
   * Output: historgram
   */
	public static class MapStage2 
        extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		double min = 0;
		double max = 0;
		double gap = 0;
		int histWidth = 1000;

		public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      min = (double)conf.getFloat("pagerank.min.pr", 0f);
      max = (double)conf.getFloat("pagerank.max.pr", 0f);
			gap = max - min;
		}

		public void map (final LongWritable key, final Text value, 
                     final Context context) 
        throws IOException, InterruptedException {
			String lineText = value.toString();
			if (lineText.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = lineText.split("\t");
			double pagerank = Double.parseDouble(line[1]);
			int distrIndex = (int)(histWidth * (pagerank - min)/gap) + 1;
			if(distrIndex == histWidth + 1)
				distrIndex = histWidth;
			context.write(new IntWritable(distrIndex) , new IntWritable(1));
		}
	}

  public static class RedStage2 
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce (final IntWritable key, 
                        final Iterable<IntWritable> values, 
                        final Context context) 
        throws IOException, InterruptedException {

			int sum = 0;

      for (IntWritable value : values) {
				int curValue = value.get();
				sum += curValue;
			}

			context.write(key, new IntWritable(sum));
    }
  }

  //////////////////////////////////////////////////////////////////////
  // command line interface
  //////////////////////////////////////////////////////////////////////

  protected Configuration conf;
  protected Path inPath = null;
  protected Path mmPath = null;
  protected Path distPath = null;

  public static void main(final String[] args) {
    try {
      final int result = ToolRunner.run(new Configuration(), 
                                        new PagerankPrep(),
                                        args);
      System.exit(result);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  protected static int printUsage() {
    System.out.println("PagerankPrep <inPath> <mmPath> <distPath>");
    return -1;
  }

  public int run(final String[] args) throws Exception {
    if (args.length != 3) {
      return printUsage();
    }

    conf = getConf();
    conf.addResource("pagerank-conf.xml");

    inPath = new Path(args[0]);
    mmPath = new Path(args[1]);
    distPath = new Path(args[2]);

    FileSystem fs = FileSystem.get(conf);
    fs.delete(mmPath, true);
    fs.delete(distPath, true);

    waitForJobFinish(configStage1());
    waitForJobFinish(configStage2());

    return 1;
  }

  private void waitForJobFinish(Job job) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new RuntimeException(job.toString());
    }
  }

  protected Job configStage1 () throws Exception {
    Job job = new Job(conf, "PagerankCheck");
    job.setJarByClass(PagerankCheck.class);
		job.setMapperClass(MapStage1.class);        
		job.setReducerClass(RedStage1.class);
		job.setCombinerClass(RedStage1.class);
		job.setNumReduceTasks( 1 );
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.setInputPaths(job, inPath);  
		FileOutputFormat.setOutputPath(job, mmPath);  
		return job;
  }

  protected Job configStage2() throws Exception {
    //read min and max value
    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream in = fs.open(mmPath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    double min = 0, max = 1;
    while(true) {
      String lineText = reader.readLine();
      if (lineText == null) break;
      String[] line = lineText.split("\t");
      if (line[0].startsWith("0"))
        min = Double.parseDouble(line[1]);
      else
        max = Double.parseDouble(line[1]);
    }
		conf.set("pagerank.min.pr", "" + min);
		conf.set("pagerank.max.pr", "" + max);
    Job job = new Job(conf, "PagerankCheck");
		job.setMapperClass(MapStage2.class);        
		job.setReducerClass(RedStage2.class);
		job.setCombinerClass(RedStage2.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, inPath);  
		FileOutputFormat.setOutputPath(job, distPath);  
    return job;
  }

}
