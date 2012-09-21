
package bench.pagerank;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.map2.IndexedTextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.StringUtils;


public class PagerankBlockPrep extends Configured implements Tool {
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
        extends Mapper<LongWritable, Text, Text, Text> {

    int blockWidth = 1;
    int numNodes = 1;

		public void setup(Context context) 
        throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      numNodes = conf.getInt("pagerank.num.nodes", -1);
      blockWidth = conf.getInt("pagerank.block.width", -1);
    }

		public void map(final LongWritable key, final Text value, 
                    final Context context) 
        throws IOException, InterruptedException {

			String lineText = value.toString();
			if (lineText.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = lineText.split("\t");
			if(line.length < 2 )
				return;

      int rowId = Integer.parseInt(line[0]);
      int colId = Integer.parseInt(line[1]);
      int blockRowId = rowId / blockWidth;
      int blockColId = colId / blockWidth;
      //int inBlockRow = rowId % blockWidth;
      //int inBlockCol = colId % blockWidth;
      //tricked transpose
      int inBlockRow = colId % blockWidth;
      int inBlockCol = rowId % blockWidth;

      context.write(new Text("" + blockRowId + "\t" + blockColId), 
                    new Text("" + inBlockRow + " " + inBlockCol + " " + line[2]) );
		}
	}


	static class MvPrepComparator implements Comparator {   
		public int compare(Object o1, Object o2) {
			String s1 = o1.toString();   
			String s2 = o2.toString();   

			int pos1 = s1.indexOf(' ');
			int pos2 = s2.indexOf(' ');

			int val1 = Integer.parseInt(s1.substring(0,pos1));
			int val2 = Integer.parseInt(s2.substring(0,pos2));

			return (val1-val2);
		}

		public boolean equals(Object o1, Object o2) {
			String s1 = o1.toString();   
			String s2 = o2.toString();   

			int pos1 = s1.indexOf(' ');
			int pos2 = s2.indexOf(' ');

			int val1 = Integer.parseInt(s1.substring(0,pos1));
			int val2 = Integer.parseInt(s2.substring(0,pos2));

			if( val1 == val2 )
				return true;
			else
				return false;
		}   
	}   

  public static class ReduceStage2 extends Reducer<Text, Text, Text, Text> {

    MvPrepComparator mpc = new MvPrepComparator();

		public void reduce (final Text key, final Iterable<Text> values, 
                        Context context) 
        throws IOException, InterruptedException {

			String out_value = "";
			ArrayList<String> value_al = new ArrayList<String>();

			for (Text val : values) {
				// vector: key=BLOCKID, value= IN-BLOCK-INDEX VALUE
				// matrix: key=BLOCK-ROW		BLOCK-COL, value=IN-BLOCK-ROW IN-BLOCK-COL VALUE

				String value_text = val.toString();
				value_al.add( value_text );
			}

			Collections.sort(value_al, mpc );

			Iterator<String> iter = value_al.iterator();
			while( iter.hasNext() ){
				String cur_val = iter.next();

				if( out_value.length() != 0 )
					out_value += " ";
				out_value += cur_val;
			}

			value_al.clear();

				context.write(key, new Text(out_value));
    }
  }

  //////////////////////////////////////////////////////////////////////
  // command line interface
  //////////////////////////////////////////////////////////////////////
  protected Path edgePath = null;
  protected Path blkEdgePath = null;
  protected Path blkNodePath = null;
  protected Path tmpPath = null;
  protected Configuration conf = null;

  // Main entry point.
  public static void main (final String[] args) throws Exception
  {
    final int result = ToolRunner.run(new Configuration(), 
                                      new PagerankBlockPrep(), args);

    System.exit(result);
  }

  // Print the command-line usage text.
  protected static int printUsage ()
  {
    System.out.println("PagerankBlockPrep <edgePath> <blkEdgePath> <blkNodePath>");

    ToolRunner.printGenericCommandUsage(System.out);

    return -1;
  }

  public void setPaths(String edge, String blkEdge, String blkNode) {
		edgePath = new Path(edge);
		blkEdgePath = new Path(blkEdge);
    blkNodePath = new Path(blkNode);
  }

  // submit the map/reduce job.
  public int run (final String[] args) throws Exception {
		if( args.length != 3 ) {
			return printUsage();
		}

    setPaths(args[0], args[1], args[2]);
    blockEdges();
    initNodes();

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

  public int initNodes() throws Exception {
    conf = getConf();
    if (conf == null) {
      conf = new Configuration();
      setConf(conf);
    }
    conf.addResource("pagerank-conf.xml");
    checkValidity();

    FileSystem fs = FileSystem.get(conf);
    fs.delete(blkNodePath, true);

    int numNodes = conf.getInt("pagerank.num.nodes", 1);
    int blockWidth = conf.getInt("pagerank.block.width", 1);
    String localPath = "/tmp/initialNodeRank";
    FileOutputStream file = new FileOutputStream(localPath);
    DataOutputStream out = new DataOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    System.out.println("generating initial rank vector");
    int mileStone = numNodes / blockWidth / 100;
    mileStone ++;
    int nextStop = 0;
    for (int i = 0; i < numNodes; i += blockWidth) {
      StringBuilder sb = new StringBuilder();
      int blockId = i / blockWidth;
      sb.append("" + blockId + "\tv");
      for (int j = 0; j < blockWidth; ++j) {
        boolean finish = false;
        if ((j == blockWidth - 1) ||
            (i + j == numNodes - 1)) {
          finish = true;
        }
        if (finish == true) {
          sb.append("" + j + " " + 1 / (float)numNodes);
          break;
        }
        else {
          sb.append("" + j + " " + 1 / (float)numNodes + " ");
        }

      }
      writer.write(sb.toString());
      writer.newLine();
      if (i > nextStop) {
        System.out.print(".");
        nextStop += mileStone;
      }
    }
    writer.close();
    System.out.print("\n");
    //copy to hdfs
    fs.copyFromLocalFile(false, true, new Path(localPath), 
                         blkNodePath);
    return 1;
  }

  private Job waitForJobFinish(Job job) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new RuntimeException(job.toString());
    }
    return job;
  }

  
  private Job configStage1() throws Exception {
    Job job = new Job(conf, "PagerankBlockPrep");
    job.setJarByClass(PagerankBlockPrep.class);
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
    Job job = new Job(conf, "PagerankBlockPrepStage2");
    job.setJarByClass(PagerankBlockPrep.class);
    job.setMapperClass(MapStage2.class);
    job.setReducerClass(ReduceStage2.class);
    job.setNumReduceTasks(conf.getInt("pagerank.num.reducers", 1));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, tmpPath);
    FileOutputFormat.setOutputPath(job, blkEdgePath);
    return job;
  }

}

