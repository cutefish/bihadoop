
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class TestCachedReadPara {
  public static final int lineWidth = 16;
  public static final String tmpDir = "/tmp";

  public static class Worker implements Runnable {

    private String inputFile;
    private int numBlocks;
    private long length;
    private boolean cacheflag;
    private String name;

    public Worker(String inputFile, int numBlocks, 
        long length, boolean cacheflag, String name) {
      this.inputFile = inputFile;
      this.numBlocks = numBlocks;
      this.length = length;
      this.cacheflag = cacheflag;
      this.name = name;
    }

    public void run() {
      try {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        conf.addResource("hdfs-site.xml");

        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(inputFile);

        FSDataInputStream in;
        if (!cacheflag) {
          in = fs.open(filePath);
        }
        else {
          in = fs.openCachedReadOnly(filePath);
        }

        int offset = (new Random()).nextInt(64000000);
        in.seek(offset);

        PrintWriter out = new PrintWriter(
            new File(tmpDir + "/" + this.name));


        for (int i = 0; i < numBlocks; ++i) {

          int numDisp = 0;
          long bytesRead = 0;
          long interval = length / 10;
          int n = 0;
          byte[] buffer = new byte[128];

          long start = System.currentTimeMillis();

          while((bytesRead < length) && (n != -1)){
            n = in.read(buffer);
            if (n == -1) break;
            bytesRead += n;
            if (bytesRead > numDisp * interval) {
              String read = (new String(buffer)).substring(0, lineWidth - 1);
              out.format("string read: %s, offset: %d\n",
                  read, bytesRead + offset);
              numDisp ++;
            }
          }
          offset += bytesRead;

          long end = System.currentTimeMillis();

          out.format("time: %d ms\n", (end - start));
          out.format("bandwidth: %d Mbytes/s\n", length / (end - start) / 1000);
          out.flush();
        }
      }
      catch(Exception e) {
        System.out.println("Exception inside worker");
        System.out.println(e);
      }
    }
  }

  public static void main(String[] args) {
    try {
      if (args.length < 4) {
        System.out.println("java TestCachedReadSeq file numThreads numBlocks length cacheflag");
        System.exit(-1);
      }

      String inputFile = "/data/" + args[0];
      int numThreads = Integer.parseInt(args[1]);
      int numBlocks = Integer.parseInt(args[2]);
      long length = Long.parseLong(args[3]);
      boolean cacheflag = false;
      try {
        cacheflag = Boolean.parseBoolean(args[4]);
      }
      catch (ArrayIndexOutOfBoundsException e) {
      }

      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < numThreads; ++i) {
        Worker worker = new Worker( inputFile, numBlocks, length, 
            cacheflag, "TestCachedReadWorker" + i);
        threads[i] = new Thread(worker);
        threads[i].start();
      }

      for (int i = 0; i < numThreads; ++i) {
        threads[i].join();
      }

    }
    catch (Exception e) {
      System.out.println(e);
    }
  }
}
