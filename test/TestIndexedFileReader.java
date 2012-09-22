import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.IndexedFileReader;

public class TestIndexedFileReader {
  public static void main(String[] args) {
    try {
      if (args.length != 2) {
        System.out.println("usage: <fsUri> <file>");
        System.exit(-1);
      }
      URI fsUri = new URI(args[0]);
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(fsUri, conf);

//      Path testPath = new Path("/test");
//      FSDataOutputStream out = fs.create(testPath, false);
//      out.writeInt(1);
//      out.writeInt(2);
//      out.close();
//      FSDataInputStream in = fs.open(testPath);
//      System.out.println(in.readInt());
//      System.out.println(in.readInt());
//      in.close();


      Path filePath = new Path(args[1]);
      IndexedFileReader reader = new IndexedFileReader();
      reader.readIndexedFile(fs, filePath);
      List<String> idxList = reader.getIndexList();
      List<Segment[]> segList = reader.getSegmentList();
      for (int i = 0; i < idxList.size(); ++i) {
        System.out.println("index: " + idxList.get(i));
        for (int j = 0; j < segList.get(i).length; ++j) {
          System.out.println("  segment: " + segList.get(i)[j]);
        }
      }
      System.out.println("size: " + idxList.size());
      System.out.println("size: " + segList.size());
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
    }
  }
}
