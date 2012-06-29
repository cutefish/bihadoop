import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.*;
import java.util.Arrays;

public class MemoryMap {

  //main test
  public static void main(String[] args) {
    try {
      FileInputStream fis = new FileInputStream("data115M");
      FileChannel fc = fis.getChannel();
      int length = 100000000;
      MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, length);
      buffer.load();
      while (true) {
        Thread.sleep(5000);
        System.out.println("wake up and sleep");
      }
//      fis.close();
    }
    catch (IOException e) {
      System.err.println(e);
    }
    catch (InterruptedException e) {
      System.err.println(e);
    }

  }

}
