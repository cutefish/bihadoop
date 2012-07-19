import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class TestCachedReadSeq {
  public static final int lineWidth = 16;

  public static void main(String[] args) {
    try {
      if (args.length < 3) {
        System.out.println("java TestCachedReadSeq file offset length cacheflag");
        System.exit(-1);
      }

      String inputFile = "/data/" + args[0];
      long offset = Long.parseLong(args[1]);
      long length = Long.parseLong(args[2]);
      boolean cacheflag = false;
      try {
        cacheflag = Boolean.parseBoolean(args[3]);
      }
      catch (ArrayIndexOutOfBoundsException e) {
      }

      Configuration conf = new Configuration();
      conf.addResource("hdfs-default.xml");
      conf.addResource("hdfs-site.xml");
      System.out.format("fs.default.name:%s\n", conf.get("fs.default.name"));
      System.out.format("dfs.replication:%d\n", conf.getInt("dfs.replication", 3));

      FileSystem fs = FileSystem.get(conf);
      Path filePath = new Path(inputFile);

      FSDataInputStream in;
      if (!cacheflag) {
        in = fs.open(filePath);
      }
      else {
        in = fs.openCachedReadOnly(filePath);
      }
      in.seek(offset);

      int n = 0;
      long bytesRead = 0;
      long interval = length / 10;
      int numDisp = 0;
      System.out.format("print interval: %d\n", interval);
      byte[] buffer = new byte[4096];

      long start = System.currentTimeMillis();

      while((bytesRead < length) && (n != -1)){
        n = in.read(buffer);
        if (n == -1) break;
        bytesRead += n;
        if (bytesRead > numDisp * interval) {
          String read = (new String(buffer)).substring(0, lineWidth - 1);
          System.out.format("string read: %s, offset: %d\n",
              read, bytesRead + offset);
          numDisp ++;
        }
      }

      long end = System.currentTimeMillis();

      System.out.format("time: %d ms\n", (end - start));
      System.out.format("bandwidth: %d Mbytes/s\n", length / (end - start) / 1000);
    }
    catch (Exception e) {
      System.out.println(e);
    }
  }
}
