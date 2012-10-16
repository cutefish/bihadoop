import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class TestCachedReadSeq {
  public static final int lineWidth = 16;

  public static void main(String[] args) {
    try {
      if (args.length < 3) {
        System.out.println("java TestCachedReadSeq <file> <offset> <length> " + 
                           "[cacheflag]");
        System.exit(-1);
      }

      String inputFile = args[0];
      long offset = Long.parseLong(args[1]);
      long length = Long.parseLong(args[2]);
      boolean cacheflag = false;
      try {
        cacheflag = Boolean.parseBoolean(args[3]);
      }
      catch (ArrayIndexOutOfBoundsException e) {
      }

      Configuration conf = new Configuration();
      //add s3 configurations
      conf.set("fs.s3.awsAccessKeyId", System.getenv().get("AWSACCESSKEYID"));
      conf.set("fs.s3n.awsAccessKeyId", System.getenv().get("AWSACCESSKEYID"));
      conf.set("fs.s3.awsSecretAccessKey", 
               System.getenv().get("AWSSECRETACCESSKEY"));
      conf.set("fs.s3n.awsSecretAccessKey",
               System.getenv().get("AWSSECRETACCESSKEY"));


      FileSystem fs = FileSystem.get(new URI(inputFile), conf);
      Path filePath = new Path(inputFile);

      FSDataInputStream in;
      if (!cacheflag) {
        in = fs.open(filePath);
        System.out.println("opened file");
      }
      else {
        System.out.println("cache opening");
        in = fs.openCachedReadOnly(filePath);
        System.out.println("cache opened file");
      }
      in.seek(offset);
      System.out.println("seeked offset");

      int n = 0;
      long bytesRead = 0;
      long interval = length / 10;
      int numDisp = 0;
      System.out.format("print interval: %d\n", interval);

      long start = System.currentTimeMillis();

      BufferedReader d = new BufferedReader(new InputStreamReader(in));
      int lineno = 0;
      while((bytesRead < length) && (n != -1)){
        String line = d.readLine();
        n = line.length() + 1;
        if (n == -1) break;
        lineno ++;
        bytesRead += n;
        if (bytesRead > numDisp * interval) {
          System.out.format("string read: %s, offset: %d, lineno: %d\n",
              line, bytesRead + offset, lineno);
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
