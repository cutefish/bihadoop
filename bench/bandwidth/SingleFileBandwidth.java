package bench.bandwidth;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.EOFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class SingleFileBandwidth {

  protected static enum PerfCounters { TIME_SPENT, BYTES_READ }

  public static class MapStage
        extends Mapper<LongWritable, Text, 
                        NullWritable, NullWritable> {

    FileSystem fs;
    Path path;
    long size;

    public void setup(Context context) 
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      path = new Path(conf.get("bandwidth.file.name"));
      fs = FileSystem.get(path.toUri(), conf);
      size = conf.getLong("bandwidth.read.size", 100*1024*1024);

    }

    public void map(final LongWritable key,
                    final Text value,
                    final Context context) 
        throws IOException, InterruptedException {
      
      System.out.println("Mapper: " + key + "\t" + value);
      System.out.println("Reading file: " + path);

      FSDataInputStream in;
      DataInputStream dataIn;

      Configuration conf = context.getConfiguration();
      
      long start, end;

      //read the B segment into memory
      start = System.currentTimeMillis();
      in = fs.open(path);
      dataIn = new DataInputStream(new BufferedInputStream(in));

      long bytesRead = 0;
      long sum = 0;
      try {
        while(bytesRead < size) {
          sum += dataIn.readByte();
          bytesRead += 4;
          if (bytesRead % 50*1024*1024 == 0) {
            context.progress();
          }
        }
      }
      catch (EOFException eof) {
      }
      in.close();
      end = System.currentTimeMillis();
      System.out.println("read size: " + bytesRead / 1024 / 1024 + " MByte");
      System.out.println("get sum: " + sum + " ms");
      System.out.println("read time: " + (end - start) + " ms");
      System.out.println("read bandwidth: " + 
                         bytesRead / (end - start) / 1000 + " MBytes/s");

      context.getCounter(PerfCounters.TIME_SPENT).increment(end - start);
      context.getCounter(PerfCounters.BYTES_READ).increment(bytesRead);
    }
  }

  /****************************************************************************
   * command line
   ***************************************************************************/

  int numMappers = 0;
  protected Path readPath = null;
  protected Path inPath = null;
  protected Path outPath = null;
  Configuration conf;

  public static void main(final String[] args) {
    try {
      SingleFileBandwidth sfb = new SingleFileBandwidth();
      sfb.run(args);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void run(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("SingleFileBandwidth <fileName> <numNodes> <size>");
      System.exit(-1);
    }
    readPath = new Path(args[0]);
    numMappers = Integer.parseInt(args[1]);
    long size = Long.parseLong(args[2]);
    inPath = new Path(readPath.getParent(), "nullin");
    outPath = new Path(readPath.getParent(), "nullout");
    conf = new Configuration();
    conf.addResource("bandwidth-conf.xml");
    conf.set("bandwidth.file.name", readPath.toString());
    conf.setLong("bandwidth.read.size", size);
    FileSystem fs = FileSystem.get(readPath.toUri(), conf);
    long start, end;

    fs.delete(inPath);
    fs.delete(outPath);

    fs.mkdirs(inPath);
    for (int i = 0; i < numMappers; ++i) {
      FSDataOutputStream out = new FSDataOutputStream(
          fs.create(new Path(inPath, "" + i)));
      (new Text("" + i)).write(out);
      out.close();
    }

    start = System.currentTimeMillis();
    Job job = waitForJobFinish(configStage());
    end = System.currentTimeMillis();

    Counters c = job.getCounters();
    long totalTime = c.findCounter(PerfCounters.TIME_SPENT).getValue();
    long totalBytes = c.findCounter(PerfCounters.BYTES_READ).getValue();

    System.out.println("total time:  " + totalTime + " ms");
    System.out.println("total bytes: " + totalBytes/1024/1024 + " MByte");
    System.out.println("average bandwidth: " + 
                       totalBytes / totalTime / 1000 + " MByte / s");
  }

  private Job waitForJobFinish(Job job) throws Exception {
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new RuntimeException(job.toString());
    }
    return job;
  }

  private Job configStage() throws Exception {
    Job job = new Job(conf, "SingleFileBandwidth");
    job.setJarByClass(SingleFileBandwidth.class);
    job.setMapperClass(MapStage.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.setInputPaths(job, inPath);
    FileOutputFormat.setOutputPath(job, outPath);
    return job;
  }

}
