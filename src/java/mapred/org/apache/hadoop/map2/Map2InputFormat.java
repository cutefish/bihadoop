package org.apache.hadoop.map2;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An InputFormat capable of accept multiple splits with indices.
 *
 * Currently only support two splits.
 */
abstract public class Map2InputFormat<K, V> extends FileInputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(Map2InputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    List<InputSplit> splits;
    List<String> idxList = new ArrayList<String>();
    List<Segment[]> segmentList = new ArrayList<Segment[]>();
    Map<Segment, String[]> segLocMap = new HashMap<Segment, String[]>();

    List<FileStatus> files = listStatus(job);
    IndexedFileReader reader = new IndexedFileReader();
    NetworkTopology clusterMap = new NetworkTopology();
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
        int blkIndex = getBlockIndex(blkLocations, 0);
        segLocMap.put(seg, blkLocations[blkIndex].getHosts());
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
            int blkIndex = getBlockIndex(blkLocations, off);
            segLocMap.put(seg, blkLocations[blkIndex].getHosts());
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
          String[][] hosts = new String[segs.length][];
          for (int k = 0; k < segs.length; ++k) {
              hosts[k] = segLocMap.get(segs[k]);
          }
          Map2Split split = new Map2Split(segs, hosts);
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
}
