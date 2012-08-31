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
File: MatvecPrep.java
 - convert matrix(edges) or vectors into block form. 
   This program is used for converting data to be used in the block version of HADI, HCC, and PageRank.
Version: 2.0
***********************************************************************/

package bench.preproc;

import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.map2.IndexedTextOutputFormat;
import org.apache.hadoop.util.*;

public class MatvecPrep extends Configured implements Tool 
{
  //////////////////////////////////////////////////////////////////////
  // STAGE 1: convert vectors and edges to block format
  //		(a) (vector)  ROWID		vVALUE    =>    BLOCKID	IN-BLOCK-INDEX VALUE
  //      (b) (real matrix)  ROWID		COLID		VALUE    
  //            =>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL VALUE
  //      (c) (0-1 matrix)  ROWID		COLID
  //            =>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL VALUE
  //////////////////////////////////////////////////////////////////////
  public static class MapStage1 extends Mapper<Object, Text, Text, Text>
  {
    int block_size;
    boolean makesym;

    public void setup(Context context) {
      block_size = context.getConfiguration().getInt("matvec.block.size", -1);
      makesym = context.getConfiguration().getBoolean("matvec.makesym", false);

      //System.out.println("MapStage1: block_size = " + block_size + ", matrix_row=" + matrix_row + ", makesym = " + makesym);
    }

    public void map (final Object key, final Text value, 
                     final Context context) 
        throws IOException, InterruptedException {

			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			if (line.length < 2)
				return;

			if (line[1].charAt(0) == 'v') {
				// (vector)  ROWID		vVALUE    =>    BLOCKID	IN-BLOCK-INDEX VALUE
				int row_id = Integer.parseInt(line[0]);
				int block_id = row_id / block_size;
				int in_block_index = row_id % block_size;

				context.write(new Text("" + block_id), 
                      new Text("" + in_block_index + " " + 
                               line[1].substring(1)) );
      } 
      else {
        int row_id = Integer.parseInt(line[0]);
        int col_id = Integer.parseInt(line[1]);
        int block_rowid = row_id / block_size;
        int block_colid = col_id / block_size;
        int in_block_row = col_id % block_size;	// trick : transpose
        int in_block_col = row_id % block_size; // trick : transpose

        if (line.length == 3) {
					//(real matrix)  ROWID		COLID		VALUE    
          //=>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL VALUE
          String elem_val;
          if(line[2].charAt(0) == 'v') 
						 elem_val = line[2].substring(1);
          else
            elem_val = line[2];

					context.write(new Text("" + block_rowid + "\t" + block_colid), 
                        new Text("" + in_block_row + " " + 
                                 in_block_col + " " + line[2]));
				} 
        else {
					//(0-1 matrix)  ROWID		COLID		
					//=>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL
					context.write(new Text("" + block_rowid + "\t" + block_colid), 
                        new Text("" + in_block_row + " " + in_block_col));

					if (makesym)	// output transposed entry
						context.write(new Text("" + block_colid + "\t" + block_rowid), 
                          new Text("" + in_block_col + " " + in_block_row));
				}
      }
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

  public static class RedStage1 extends Reducer<Text, Text, Text, Text> {
    String out_prefix = "";
    MvPrepComparator mpc = new MvPrepComparator();

		public void setup(Context context) {
			out_prefix = context.getConfiguration().get("out_prefix");
		}

		public void reduce (final Text key, final Iterable<Text> values, 
                        final Context context) 
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

      if( out_prefix != null )
        context.write(key, new Text(out_prefix + out_value));
      else
        context.write(key, new Text(out_value));
    }
  }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
    protected Path output_path = null;
    protected String baseName = null;
    protected int number_nodes = 0;
    protected int block_size = 1;
    protected int nreducer = 1;
    protected String output_prefix;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
      final int result = ToolRunner.run(new Configuration(), new MatvecPrep(), args);

      System.exit(result);
    }

    // Print the command-line usage text.
    protected static int printUsage ()
    {
      System.out.println("MatvecPrep <edge_path> <outputedge_path>");

      ToolRunner.printGenericCommandUsage(System.out);

      return -1;
    }

    // submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
      if (args.length < 2) {
        return printUsage();
      }

      edge_path = new Path(args[0]);
      output_path = new Path(args[1]);				

      if (args.length >= 3) {
        baseName = args[2];
      }

      // run job
      Job job = configStage1();
      return job.waitForCompletion(true) ? 0 : 1;
    }

    // Configure pass1
    protected Job configStage1 () throws Exception {
      Configuration conf = new Configuration();
      conf.addResource("bench-common.xml");
      conf.addResource("preproc-default-conf.xml");
      System.out.format("common: fs.default.name:%s\n", conf.get("fs.default.name"));

      Job job = new Job(conf, "MatvecPrep");
      checkValidity(job);
      System.out.format("matvec.block.size:%d\n",
                        job.getConfiguration().getInt(
                            "matvec.block.size", -1));
      System.out.format("matvec.num.reduce:%d\n",
                        job.getConfiguration().getInt(
                            "matvec.num.reduce", -1));
      System.out.format("matvec.makesym:%b\n",
                        job.getConfiguration().getBoolean(
                            "matvec.makesym", false));
      System.out.format("matvec.buildindex:%b\n",
                        job.getConfiguration().getBoolean(
                            "matvec.buildindex", true));

      job.setJarByClass(MatvecPrep.class);
      job.setMapperClass(MapStage1.class);        
      job.setReducerClass(RedStage1.class);
      job.setNumReduceTasks(job.getConfiguration().getInt(
              "matvec.num.reduce", -1));
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      if (job.getConfiguration().getBoolean(
              "matvec.buildindex", true)) {
        job.setOutputFormatClass(MatvecOutputFormat.class);
      }

      FileSystem fs = FileSystem.get(job.getConfiguration());
      fs.delete(output_path, true);

      FileInputFormat.setInputPaths(job, edge_path);  
      FileOutputFormat.setOutputPath(job, output_path);  

      return job;
    }

    private void checkValidity(Job job) {
      if (job.getConfiguration().getInt(
              "matvec.block.size", -1) == -1) {
        System.err.println("matvec.block.size not set");
        System.exit(-1);
      }
      if (job.getConfiguration().getInt(
              "matvec.num.reduce", -1) == -1) {
        System.err.println("matvec.num.reduce not set");
        System.exit(-1);
      }
    }

    public static class MatvecOutputFormat<K, V> 
          extends IndexedTextOutputFormat<K, V> {
      @Override
      protected <K, V> String generateIndexForKeyValue(
          K key, V value, String path) {
        String keyString = key.toString();
        System.out.println("key: " + keyString);
        System.out.println("value: " + value.toString());
        System.out.println("path: " + path);
        return path + "#blockid#" + keyString;
      }
    }
}


