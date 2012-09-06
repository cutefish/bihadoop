import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Segments;

public class TestReadMetaInfo {
  private List<Segment[]> segList = new ArrayList<Segment[]>();
  private Map<String, List<Segment>> localityMap = 
      new HashMap<String, List<Segment>>();

  public void readMetaInfo(Path jobSubmitDir) 
      throws IOException {

    FileSystem fs = FileSystem.get(new Configuration());
    Path infoPath = new Path(jobSubmitDir, "job.splitmap2metainfo");
    FSDataInputStream in = fs.open(infoPath);
    System.out.println("infoPath: " + infoPath.toString());
    byte[] HEADER = "MAP2-INFO".getBytes("UTF-8");
    byte[] header = new byte[HEADER.length]; 
    org.apache.hadoop.fs.FileStatus fstatus = fs.getFileStatus(infoPath);
    System.out.println("file:" + 
                       " length: " + fstatus.getLen() + 
                       " isdir: " + fstatus.isDir() + 
                       " block size: " + fstatus.getBlockSize() + 
                       " permission: " + fstatus.getPermission() + 
                       " owner: " + fstatus.getOwner() + 
                       " path: " + fstatus.getPath() + 
                       " \n");
    in.readFully(header);
    if (!Arrays.equals(HEADER, header)) {
      throw new IOException("Invalid header on map2 info file");
    }

    int numSplits = WritableUtils.readVInt(in);
    System.out.println("num splits: " + numSplits);
    for (int i = 0; i < numSplits; ++i) {
      int numSegs = WritableUtils.readVInt(in);
      //currently only support pair, thus only process first two segments.
      if (numSegs != 2) {
        throw new IOException("Number of segments each split other than 2");
      }
      Segment[] segs = new Segment[2];
      for (int j = 0; j < 2; ++j) {
        segs[j] = new Segment();
        segs[j].readFields(in);
        int numLocs = WritableUtils.readVInt(in);
        for (int k = 0; k < numLocs; ++k) {
          String loc = Text.readString(in);
          List<Segment> localList = localityMap.get(loc);
          if (localList == null) {
            localList = new ArrayList<Segment>();
            localityMap.put(loc, localList);
          }
          //insert with order, assuming segments do not overlap
          int idx = Collections.binarySearch(localList, segs[j]);
          if (idx < 0) {
            idx = -(idx + 1);
            localList.add(idx, segs[j]);
          }
        }
      }
      segList.add(segs);
    }
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("segment list:\n");
    for (Segment[] segs : segList) {
      sb.append("[");
      for (Segment seg : segs) {
        sb.append(seg.toString() + ", ");
      }
      sb.append("]\n");
    }
    sb.append("\nloc map:\n");
    for (String loc : localityMap.keySet()) {
      sb.append(loc + "[");
      for (Segment seg: localityMap.get(loc)) {
        sb.append(seg.toString() + ", ");
      }
      sb.append("]\n");
    }
    return sb.toString();
  }

  public static void main(String[] args) {
    try {
      if (args.length < 1) {
        System.exit(-1);
      }
      Path p = new Path(args[0]);
      TestReadMetaInfo info = new TestReadMetaInfo();
      info.readMetaInfo(p);
      System.out.println(info.toString());
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
