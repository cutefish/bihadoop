package org.apache.hadoop.map2;

import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.core.fs.Segment;
import org.apache.hadoop.mapreduce.lib.input;
import org.apache.hadoop.net.NetworkTopology;

/**
 * An InputFormat capable of accept multiple splits with indices.
 *
 * Currently only support two splits.
 */
public class Map2InputFormat<K, V> extends FileInputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(Map2InputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    List<InputSplit> splits;
    List<String> idxList = new ArrayList<String>();
    List<Segment[]> segmentList = new ArrayList<Segment[]>();
    Map<Segment, String[]> segLocMap = new HashMap<Segment, String[]>();

    List<FileStatus> files = listStatus(job);
    IndexedFileReader reader;
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job.getConfiguration());
      long length = file.getLen();
      BlockLocation[] blkLocations = 
          fs.getFileBlockLocations(file, 0, length);
      if (fileNameAsIndex(job)) {
        //use file name as index, total file as the segment
        indices.add(path.toString());
        Segment seg = new Segment(path, 0, length);
        segmentList.add(new Segment[] { seg });
        String[] splitHosts = getSplitHosts(blkLocations, 
                                            0, length, clusterMap);
        segLocMap.put(seg, splitHosts);
      }
      else {
        reader.readIndexedFile(fs, path);
        //add to list
        indices.addAll(reader.getIndexList());
        List<Segment[]> indexedSegments = reader.getSegmentList();
        segmentList.addAll(indexedSegments);
        //get locations for all segments
        for (Segment[] segs : indexedSegments) {
          ArrayList<String> locations = new ArrayList<String>();
          for (Segment seg : segs) {
            long off = seg.getOffset();
            long len = seg.getLength();
            String[] splitHosts = getSplitHosts(blkLocations, off, 
                                                len, clusterMap);
            segLocMap.put(seg, splitHosts);
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
          String[][] hosts = new String[segs.length];
          for (int i = 0; i < segs.length; ++i) {
              hosts[i] = segLocMap.get(segs[i]);
          }
          Map2Split split = new Map2Split(segs, hosts);
          splits.add(split);
        }
      }
    }
  }

  private static T[] combineArray(T[] a, T[] b) {
    int length = a.length + b.length;
    T[] ret = new T[length];
    System.arraycopy(a, 0, ret, 0, a.length);
    System.arraycoopy(b, 0, a.length, b.length);
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

  public Map<Segment, String[]> getSegmentLocations() {
    return segLocations;
  }

  private boolean fileNameAsIndex(JobContext job) {
    Configuraiton conf = context.getConfiguration();
    return conf.getBoolean("mapred.map2.input.fileNameAsIndex", false);
  }
}
