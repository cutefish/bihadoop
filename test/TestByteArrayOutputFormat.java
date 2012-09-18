import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.map2.*;

public class TestByteArrayOutputFormat {

  public static void main(String[] args) {
    try {
      if (args.length < 1) {
        System.out.println("java TestByteArrayOutputFormat <size>");
        System.exit(-1);
      }

      //generate byte array
      int size = Integer.parseInt(args[0]);
      int bytesPerUnit = 4 + 8;
      int totalBytes = bytesPerUnit * size;
      ByteBuffer bbuf = ByteBuffer.allocate(totalBytes);
      int[] keys = new int[size];
      double[] values = new double[size];
      for (int i = 0; i < size; ++i) {
        keys[i] = i;
        values[i] = 1.0 / i;
      }
      for (int i = 0; i < size; ++i) {
        bbuf.putInt(keys[i]);
        bbuf.putDouble(values[i]);
      }

      //outputformat initailization

      IndexedByteArrayOutputFormat output = new IndexedByteArrayOutputFormat();
      RecordWriter writer = output.getRecordWriter("/home/xyu40/t");
      writer.write("test", bbuf.array());

      FileInputStream fis = new FileInputStream("/home/xyu40/t");
      DataInputStream dis = new DataInputStream(fis);
      int bytesRead = 0;
      while (bytesRead < totalBytes) {
        System.out.println("" + dis.readInt() + "\t" + dis.readDouble());
        //System.out.println("" + dis.readInt());
        bytesRead += bytesPerUnit;
      }
    }
    catch (Exception e) {
      System.out.println(e);
    }
  }
}
