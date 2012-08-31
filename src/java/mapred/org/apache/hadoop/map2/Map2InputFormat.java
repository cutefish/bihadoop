package org.apache.hadoop.map2;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An InputFormat capable of accept multiple splits with indices.
 *
 * Currently only support two splits.
 */
public class Map2InputFormat 
  extends FileInputFormat<String[], TrackedSegments>  {

  private static final Log LOG = LogFactory.getLog(Map2InputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    List<InputSplit> splits;
    List<String> idxList = new ArrayList<String>();
    List<Segment[]> segmentList = new ArrayList<Segment[]>();
    Map<Segment, String[]> segLocMap = new HashMap<Segment, String[]>();

    List<FileStatus> files = listStatus(job);
    IndexedFileReader reader = new IndexedFileReader();
    for (FileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job.getConfiguration());
      long length = file.getLen();
      BlockLocation[] blkLocations = 
          fs.getFileBlockLocations(file, 0, length);
      if (fileNameAsIndex(job)) {
        //use file name as index, total file as the segment
        idxList.add(path.toString());
        Segment seg = new Segment(path, 0, length);
        segmentList.add(new Segment[] { seg });
        String[] hosts = collectHosts(blkLocations, 0, length);
        segLocMap.put(seg, hosts);
      }
      else {
        reader.readIndexedFile(fs, path);
        //add to list
        idxList.addAll(reader.getIndexList());
        List<Segment[]> indexedSegments = reader.getSegmentList();
        segmentList.addAll(indexedSegments);
        //get locations for all segments
        for (Segment[] segs : indexedSegments) {
          ArrayList<String> locations = new ArrayList<String>();
          for (Segment seg : segs) {
            long off = seg.getOffset();
            long len = seg.getLength();
            String[] hosts = collectHosts(blkLocations, 0, length);
            segLocMap.put(seg, hosts);
          }
        }
      }
    }

    //filtering through the index list and construct Map2Splits

    //To do: this filtering interface is where the restriction of two input is
    //enforced. Maybe use a parser in the future

    splits = filterAndCombine(job, idxList, segmentList, segLocMap);

    return splits;
  }

  private String[] collectHosts(BlockLocation[] blkLocations,
                                long off, long len) throws IOException {
    List<BlockLocation> blocks = Arrays.asList(blkLocations);
    List<String> hosts = new ArrayList<String>();
    long start = off;
    long end = start + off;
    int startIdx = findBlock(blocks, off);
    int idx = (startIdx > 0) ? startIdx : -(startIdx + 1);
    while(idx < blocks.size()) {
      BlockLocation curr = blocks.get(idx);
      long currStart = curr.getOffset();
      long currEnd = currStart + curr.getLength();
      if ((start <= currStart && currEnd <= end) ||
          (currStart <= start && end <= currEnd)) {
        hosts.addAll(Arrays.asList(blkLocations[idx].getHosts()));
      }
      else {
        break;
      }
      idx ++;
    }
    return hosts.toArray(new String[hosts.size()]);
  }

  private int findBlock(List<BlockLocation> list, long off) {
    BlockLocation key = new BlockLocation(null, null, off, 1);
    Comparator<BlockLocation> comp = 
        new Comparator<BlockLocation>() {
          //Returns 0 iff a is inside b or b is inside a
          public int compare(BlockLocation a, BlockLocation b) {
            long aBeg = a.getOffset();
            long bBeg = b.getOffset();
            long aEnd = aBeg + a.getLength();
            long bEnd = bBeg + b.getLength();
            if ((aBeg <= bBeg && bEnd <= aEnd) ||
                (bBeg <= aBeg && aEnd <= bEnd))
              return 0;
            if (aBeg < bBeg)
              return -1;
            return 1;
          }
        };
    return Collections.binarySearch(list, key, comp);
  }

  protected List<InputSplit> filterAndCombine(
      JobContext job, 
      List<String> idxList, List<Segment[]> segmentList, 
      Map<Segment, String[]> segLocMap) {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    Map2Filter filter = getIndexFilter(job);
    for (int i = 0; i < idxList.size(); ++i) {
      for (int j = i; j < idxList.size(); ++j) {
        if (filter.accept(idxList.get(i), idxList.get(j))) {
          Segment[] segs = combineArray(
              segmentList.get(i), segmentList.get(j));
          String[] indices = new String[segs.length];
          for (int k = 0; k < segmentList.get(i).length; ++k) {
            indices[k] = idxList.get(i);
          }
          for (int k = 0; k < segmentList.get(j).length; ++k) {
            indices[k + segmentList.get(i).length] = idxList.get(j);
          }
          String[][] hosts = new String[segs.length][];
          for (int k = 0; k < segs.length; ++k) {
              hosts[k] = segLocMap.get(segs[k]);
          }
          Map2Split split = new Map2Split(segs, indices, hosts);
          splits.add(split);
        }
      }
    }
    return splits;
  }

  private static Segment[] combineArray(Segment[] a, Segment[] b) {
    int length = a.length + b.length;
    Segment[] ret = new Segment[length];
    System.arraycopy(a, 0, ret, 0, a.length);
    System.arraycopy(b, 0, ret, a.length, b.length);
    return ret;
  }

  public static void setIndexFilter(Job job, 
                                    Class<? extends Map2Filter> filter) {
    job.getConfiguration().setClass("mapred.map2.input.indexFilter.class",
                                    filter, Map2Filter.class);
  }

  public static Map2Filter getIndexFilter(JobContext context) {
    Configuration conf = context.getConfiguration();
    Class<? extends Map2Filter> filterClass = conf.getClass(
        "mapred.map2.input.indexFilter.class", null, Map2Filter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  private boolean fileNameAsIndex(JobContext context) {
    Configuration conf = context.getConfiguration();
    return conf.getBoolean("mapred.map2.input.fileNameAsIndex", false);
  }
  
  public RecordReader<String[], TrackedSegments> createRecordReader(
      InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new Map2RecordReader();
  }

}
