import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.net.Socket;

public class FileCacheClient {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: arg1");
      System.exit(1);
    }
    Socket s = null;
    DataOutputStream out = null;
    try {
      s = new Socket("localhost", 4444);
      out = new DataOutputStream(s.getOutputStream());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    String outString = args[0];
    byte[] buffer = outString.getBytes();
    while(true) {
      try {
        out.write(buffer);
        Thread.sleep(10000);
      }
      catch (Exception e) {
        System.out.println(e);
      }
    }
  }
}
