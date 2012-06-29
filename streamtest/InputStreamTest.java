import java.io.*;
import java.util.Arrays;

public class InputStreamTest {
  /* Test FileInputStream speed.
   * Return Mbytes per second.
   */
  public long testFileInputStream(String fileName, long readSize, 
                                  int iteration, boolean countLatency) {
    try {
      long startTimeOut = System.currentTimeMillis();
      long durationIn = 0;
      for (int i = 0; i < iteration; ++i) {
        FileInputStream fis = new FileInputStream(fileName);
        long startTimeIn = System.currentTimeMillis();
        long bytesRead = 0;
        long countCR = 0; 
        int b;
        while(((b = fis.read()) != -1) && (bytesRead < readSize)) {
          if (b == '\n') countCR ++;
          bytesRead ++;
        }
        long endTimeIn = System.currentTimeMillis();
        durationIn += endTimeIn - startTimeIn;
        fis.close();
      }
      long endTimeOut = System.currentTimeMillis();
      long durationOut = endTimeOut - startTimeOut;
      if (countLatency) return (readSize) / (durationOut / iteration);
      else return (readSize) / (durationIn / iteration);
    }
    catch (IOException e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }

  /* Test BufferedInputStream speed. */
  public long testBufferedInputStream(String fileName, long readSize, 
                                      int iteration, boolean countLatency) {
    try {
      long startTimeOut = System.currentTimeMillis();
      long durationIn = 0;
      for (int i = 0; i < iteration; ++i) {
        FileInputStream fis = new FileInputStream(fileName);
        BufferedInputStream bis = new BufferedInputStream(fis);
        long startTimeIn = System.currentTimeMillis();
        long bytesRead = 0;
        long countCR = 0;
        int b;
        while(((b = bis.read()) != -1) && (bytesRead < readSize)) {
          bytesRead ++;
          if (b == '\n') countCR ++;
        }
        long endTimeIn = System.currentTimeMillis();
        durationIn += endTimeIn - startTimeIn;
        fis.close();
      }
      long endTimeOut = System.currentTimeMillis();
      long durationOut = endTimeOut - startTimeOut;
      if (countLatency) return (readSize) / (durationOut / iteration);
      else return (readSize) / (durationIn / iteration);
    }
    catch (IOException e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }

  /* Test direct buffering speed */
  public long testDirectBuffering(String fileName, long readSize, 
                                  int iteration, boolean countLatency, 
                                  long bufferSize) {
    try {
      long startTimeOut = System.currentTimeMillis();
      long durationIn = 0;
      for (int i = 0; i < iteration; ++i) {
        FileInputStream fis = new FileInputStream(fileName);
        byte[] buffer = new byte[(int)bufferSize];

        long startTimeIn = System.currentTimeMillis();

        long bytesRead = 0;
        long countCR = 0;
        int n;
        while (((n = fis.read(buffer)) != -1) && (bytesRead < readSize)) {
          for (int j = 0; j < bufferSize; ++j) {
            bytesRead ++;
            if (buffer[j] == '\n') countCR ++;
          }
        }
        long endTimeIn = System.currentTimeMillis();

        durationIn += endTimeIn - startTimeIn;

        fis.close();
      }
      long endTimeOut = System.currentTimeMillis();
      long durationOut = endTimeOut - startTimeOut;
      if (countLatency) return (readSize) / (durationOut / iteration);
      else return (readSize) / (durationIn / iteration);
    }
    catch (IOException e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }

  /* Test file copy speed */
  public long testFileCopy(String fileName, long readSize, 
                             int iteration, long bufferSize) {
    try {
      long durationIn = 0;
      for (int i = 0; i < iteration; ++i) {
        FileInputStream fis = new FileInputStream(fileName);
        byte[] buffer = new byte[(int)bufferSize];

        long startTimeIn = System.currentTimeMillis();

        long bytesRead = 0;
        long countCR = 0;
        int n;
        while (((n = fis.read(buffer)) != -1) && (bytesRead < readSize)) {
            bytesRead += n;
            if (buffer[n -i - 1] == '\n') countCR ++;
        }
        long endTimeIn = System.currentTimeMillis();

        durationIn += endTimeIn - startTimeIn;

        fis.close();
      }
      return (readSize) / (durationIn / iteration);
    }
    catch (IOException e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }

  /* Test memory copy speed */
  public long testMemCopy(String fileName, long readSize, 
                          int iteration, long bufferSize) {
    try {
      long durationIn = 0;
      for (int i = 0; i < iteration; ++i) {
        FileInputStream fis = new FileInputStream(fileName);
        byte[] buffer = new byte[(int)bufferSize];
        byte[] copy = new byte[(int)bufferSize];

        long bytesRead = 0;
        long countCR = 0;
        int n;
        while (((n = fis.read(buffer)) != -1) && (bytesRead < readSize)) {
            bytesRead += n;
            long startTimeIn = System.currentTimeMillis();
            copy = Arrays.copyOf(buffer, (int)bufferSize);
            long endTimeIn = System.currentTimeMillis();
            durationIn += endTimeIn - startTimeIn;
            if (copy[n - i - 1] == '\n') countCR ++;
        }

        fis.close();
      }
      return (readSize) / (durationIn / iteration);
    }
    catch (IOException e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }

  /* Test memory copy speed */
  public long testMemMap(String fileName, long readSize, 
                          int iteration, long bufferSize) {
    try {
      long durationIn = 0;
      for (int i = 0; i < iteration; ++i) {
        FileInputStream fis = new FileInputStream(fileName);
        byte[] buffer = new byte[(int)bufferSize];
        byte[] copy = new byte[(int)bufferSize];

        long bytesRead = 0;
        long countCR = 0;
        int n;
        while (((n = fis.read(buffer)) != -1) && (bytesRead < readSize)) {
            bytesRead += n;
            long startTimeIn = System.currentTimeMillis();
            copy = Arrays.copyOf(buffer, (int)bufferSize);
            long endTimeIn = System.currentTimeMillis();
            durationIn += endTimeIn - startTimeIn;
            if (copy[n - i - 1] == '\n') countCR ++;
        }

        fis.close();
      }
      return (readSize) / (durationIn / iteration);
    }
    catch (IOException e) {
      System.err.println(e);
      throw new RuntimeException(e);
    }
  }


  private enum TestName {
    FIS, BIS, DB, FC, MC
  }

  //main test
  public static void main(String[] args) {
    //check args number
    if (args.length < 5) {
      System.err.println("Usage: test_name file_name read_size iteration param");
      System.err.println("TestNames: FIS, BIS, DB, FC, MC");
      System.err.println("readSize: in KB");
      System.err.println("param: (true,false)/(bufferSize in kB)");
      System.exit(1);
    }
    TestName name = null;

    try {
      name = TestName.valueOf(args[0]);
    }
    catch (IllegalArgumentException e) {
      System.err.println("Test name incorrect.");
      System.exit(1);
    }
    String fileName = args[1];
    long readSize = Long.parseLong(args[2]) * 1024;
    int iteration = Integer.parseInt(args[3]);
    boolean countLatency = false;
    long bufferSize = 0;
    long kbytesPerSec = 0;
    InputStreamTest test = new InputStreamTest();
    System.out.format("test: %s %dK%n", args[0], readSize / 1024);
    switch (name) {
      case FIS: 
        countLatency = Boolean.parseBoolean(args[4]);
        System.out.format("countLatency: %b%n", countLatency);
        kbytesPerSec = test.testFileInputStream(fileName, readSize,
                                           iteration, countLatency);
        break;
      case BIS:
        countLatency = Boolean.parseBoolean(args[4]);
        System.out.format("countLatency: %b%n", countLatency);
        kbytesPerSec = test.testBufferedInputStream(fileName, readSize,
                                               iteration, countLatency);
        break;
      case DB:
        countLatency = Boolean.parseBoolean(args[4]);
        try {
          bufferSize = Integer.parseInt(args[5]) * 1024;
        }
        catch (ArrayIndexOutOfBoundsException e) {
          bufferSize = 8 * 1024;
        }
        System.out.format("bufferSize: %d%n", bufferSize);
        kbytesPerSec = test.testDirectBuffering(fileName, readSize,
                                           iteration, countLatency, bufferSize);
        break;
      case FC:
        bufferSize = Integer.parseInt(args[4]) * 1024;
        System.out.format("bufferSize: %d%n", bufferSize);
        kbytesPerSec = test.testFileCopy(fileName, readSize,
                                         iteration, bufferSize);
        break;
      case MC:
        bufferSize = Integer.parseInt(args[4]) * 1024;
        System.out.format("bufferSize: %d%n", bufferSize);
        kbytesPerSec = test.testMemCopy(fileName, readSize,
                                         iteration, bufferSize);
        break;

    }
    System.out.format("KBytesPerSec: %d%n", kbytesPerSec);
  }

}
