package org.apache.hadoop.map2;

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

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Segments;

/** 
 * A utility that reads the necessary info for scheduling, which includes:
 *
 * * The list of segment set(currently only pair) corresponding to the splits;
 * * Mapping for segment set(currently pair) to the split index;
 * * Mapping for a host to a segment.
 */

public class Map2MetaInfo {

  private static final Log LOG = LogFactory.getLog(Map2MetaInfo.class);

  //ToDo: support set instead of pair
  private List<Segment[]> segList;
  private Map<SegmentPair, Integer> segTaskMap;
  private Map<String, List<Segment>> localityMap;

  public Map2MetaInfo() {
    segList = new ArrayList<Segment[]>();
    segTaskMap = new HashMap<SegmentPair, Integer>();
    localityMap = new HashMap<String, List<Segment>>();
  }

  public void readMetaInfo(JobID jobId, FileSystem fs, 
                           Configuration conf, Path jobSubmitDir) 
      throws IOException {
    Path infoPath = JobSubmissionFiles.getJobMap2MetaFile(jobSubmitDir);
    FSDataInputStream in = fs.open(infoPath);
    byte[] HEADER = "MAP2-INFO".getBytes("UTF-8");
    byte[] header = new byte[HEADER.length]; 
    in.readFully(header);
    if (!Arrays.equals(HEADER, header)) {
      throw new IOException("Invalid header on map2 info file");
    }

    int numSplits = WritableUtils.readVInt(in);
    for (int i = 0; i < numSplits; ++i) {
      int numSegs = WritableUtils.readVInt(in);
      //currently only support pair, thus only process first two segments.
      if (numSegs != 2) {
        throw new IOException("Number of segments each split other than 2");
      }
      Segment[] segs = new Segment[2];
      for (int j = 0; j < 2; ++j) {
        segs[j].readFields(in);
        int numLocs = WritableUtils.readVInt(in);
        for (int k = 0; k < numLocs; ++k) {
          String loc = Text.readString(in);
          List<Segment> localList = localityMap.get(loc);
          if (localList == null) {
            localList = new ArrayList<Segment>();
          }
          //insert with order, assuming segments do not overlap
          int idx = Collections.binarySearch(localList, segs[j]);
          if (idx < 0) {
            idx = -(idx + 1);
            localList.add(idx, segs[j]);
          }
        }
      }
      SegmentPair pair = new SegmentPair(segs[0], segs[1]);
      segList.add(segs);
      segTaskMap.put(pair, i);
    }
  }

  public static class SegmentPair {

    Segment seg0;
    Segment seg1;
    
    public SegmentPair(Segment s0, Segment s1) {
      if (s0.compareTo(s1) < 0) {
        seg0 = s0;
        seg1 = s1;
      }
      else {
        seg1 = s0;
        seg0 = s1;
      }
    }

    static boolean isEqual(Object a, Object b) {
      return a == b || (a != null & a.equals(b));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj != null && obj instanceof SegmentPair) {
        SegmentPair that = (SegmentPair)obj;
        return isEqual(this.seg0, that.seg0)
            && (this.seg1 == that.seg1);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return seg0.hashCode() + seg1.hashCode();
    }
  }

  public int getTaskIndex(Segment seg0, Segment seg1) {
    SegmentPair pair = new SegmentPair(seg0, seg1);
    return segTaskMap.get(pair);
  }

  public List<Segment[]> getSegmentList() {
    return segList;
  }

  public Segments getLocalSegments(String host) {
    List<Segment> list = localityMap.get(host);
    if (list == null) return new Segments(new ArrayList<Segment>(0));
    return new Segments(list);
  }
  

}
