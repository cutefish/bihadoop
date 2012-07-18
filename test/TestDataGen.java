import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class TestDataGen {

  private static final int lineWidth = 16;

  enum GenFunc {
    genCounterTextFile,
    checkCounterTextFile,
  }

  public static void genCounterTextFile(String fileName, long numBytes) 
    throws IOException{
    //write each line in format xxxxxxx\n
    //where xxxxxxx represents that amount of kB written.
    long bytesWrite = 0;
    int stringWidth = lineWidth - 1;
    FileOutputStream out = new FileOutputStream(fileName);
    while (bytesWrite < numBytes) {
      bytesWrite += lineWidth;
      String line = long2StrFixedWidth(bytesWrite, stringWidth) + '\n';
      out.write(line.getBytes());
    }
  }

  private static String long2StrFixedWidth(long num, int width) {
    //mod to constraint the range
    num = num % ((long)(Math.pow(10, width + 1) - 1));
    String s = Long.toString(num);
    int paddingLength = width - s.length();
    char[] padding = new char[paddingLength];
    Arrays.fill(padding, '0');
    return (new String(padding)) + s;
  }

  public static void checkCounterTextFile(String fileName) 
    throws IOException {
    int n = 0;
    long bytesRead = 0;
    byte[] buffer = new byte[lineWidth];
    FileInputStream in = new FileInputStream(fileName);
    while (n != -1) {
      int pos = 0;
      while (pos < lineWidth) {
        n = in.read(buffer, pos, buffer.length);
        if (n == -1) {
          break;
        }
        pos += n;
        bytesRead += n;
      }
      if (n == -1) {
        break;
      }
      String read = (new String(buffer)).substring(0, lineWidth - 1);
      String expect = long2StrFixedWidth(bytesRead, lineWidth - 1);
      if ( !expect.equals(read)) {
        System.out.format("check fail, expect:%s, read:%s\n", expect, read);
        break;
      }
    }
    System.out.format("check success\n");
  }

  public static void main(String[] args) {
    try {
      if (args.length != 3) {
        System.out.println("java TestDataGen gen file size");
        System.exit(-1);
      }
      GenFunc genFunc = GenFunc.valueOf(args[0]);
      String fileName = args[1];
      long size = Long.parseLong(args[2]);

      switch (genFunc) {
        case genCounterTextFile:
          genCounterTextFile(fileName, size);
          break;
        case checkCounterTextFile:
          checkCounterTextFile(fileName);
          break;
        default:
          System.out.println("Invalid function name");
      }
    }
    catch (Exception e) {
      System.out.println(e);
    }
  }
}
