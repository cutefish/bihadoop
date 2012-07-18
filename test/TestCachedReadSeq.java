import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class TestCachedReadSeq {
  public static final int lineWidth = 16;
  public static final String inputFile = "counterTxtFile";

  //Return smallest number after m which can divide n
  private static long nextDiv(int n, long m) {
    long q = m / n;
    if (m % n == 0) {
      return m;
    }
    return (q + 1) * n;
  }

  public static void main(String[] args) {
    try {
      if (args != 2) {
        System.out.println("java TestCachedReadSeq offset length);
        System.exit(-1);
      }

      long offset = Long.parseLong(args[0]);
      long length = Long.parseLong(args[1]);

      Configuration conf = new Configuration();
      conf.addResource("hdfs-site.xml");
      conf.addResource("hdfs-default.xml");
      System.out.format("fs.default.name:%s\n", conf.get("fs.default.name"));
      System.out.format("dfs.replication:%d\n", conf.getInt("dfs.replication", 3));

      FileSystem fs = FileSystem.get(conf);
      Path filePath = new Path(inputFile);

      FSDataInputStream in = fs.open(filePath);
      //FSDataInputStream in = fs.openCachedReadOnly(filePath);
      in.seek(offset);

      int n = 0;
      long bytesRead = 0;
      long interval = nextDiv(length / 10, lineWidth);
      byte[] buffer = new byte[128];
      while((bytesRead < length) && (n != -1)){
        n = in.read(buffer);
        if (n == -1) break;
        if (bytesRead % interval == 0) {
          String read = (new String(buffer)).substring(0, lineWidth - 1);
          System.out.format("string read: %s, bytes read: %d\n",
              read, bytesRead);
        }
        bytesRead += n;
      }
    }
    catch (Exception e) {
      System.out.println(e);
    }
  }
}
