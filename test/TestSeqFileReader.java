import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;

public class TestSeqFileReader {
  public static void main(String[] args) {
    try {
      if (args.length != 3) {
        System.out.println("usage: <fsUri> <file> <isNodeType>");
        System.exit(-1);
      }
      URI fsUri = new URI(args[0]);
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(fsUri, conf);
      Path filePath = new Path(args[1]);
      boolean isNodeType = Boolean.parseBoolean(args[2]);

      SequenceFile.Reader in = new SequenceFile.Reader(fs, filePath, conf);
      System.out.println("key class name: " + 
                         in.getKeyClassName());
      System.out.println("value class name: " + 
                         in.getValueClassName());
      BytesWritable key = new BytesWritable();
      BytesWritable val = new BytesWritable();
      DataOutputBuffer buffer = new DataOutputBuffer();
      SequenceFile.ValueBytes vbytes = in.createValueBytes();
      while(in.nextRawKey(buffer) != -1) {
        key.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        in.nextRawValue(vbytes);
        vbytes.writeUncompressedBytes(buffer);
        val.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        ByteBuffer valBuf = ByteBuffer.wrap(val.getBytes());
        int numBytes = val.getLength();
        while(numBytes > 0) {
          if (isNodeType) {
            int rowId = valBuf.getInt();
            double rank = valBuf.getDouble();
            System.out.println("" + rowId + "\t" + rank + "\n");
            numBytes -= 4 + 8;
          }
          else {
            int rowId = valBuf.getInt();
            int colId = valBuf.getInt();
            double rank = valBuf.getDouble();
            System.out.println("" + rowId + "\t" + 
                               colId + "\t" + 
                               rank + "\n");
            numBytes -= 4 + 8 + 8;
          }
        }
      }
      in.close();
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
    }
  }
}
