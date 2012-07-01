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
    DataInputStream in = null;
    DataOutputStream out = null;
    String outString = args[0];
    byte[] buffer = outString.getBytes();
    while(true) {
      try {
        s = new Socket("localhost", 4444);
        out = new DataOutputStream(s.getOutputStream());
        in = new DataInputStream(s.getInputStream());
        out.write(buffer);
        byte[] inBuffer = new byte[1024];
        in.read(inBuffer);
        String path = new String(inBuffer);
        System.out.println(path);
        Thread.sleep(10000);
        s.close();
      }
      catch (Exception e) {
        System.out.println(e);
      }
    }
  }
}
