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

    LOG.info("Calculating splits");
    job.getConfiguration().setBoolean("mapred.map2.enabledMap2", true);

    List<InputSplit> splits;
    List<String> idxList = new ArrayList<String>();
    List<Segment[]> segmentList = new ArrayList<Segment[]>();
    Map<Segment, Segment> coverSegMap = new HashMap<Segment, Segment>();
    Map<Segment, String[]> segLocMap = new HashMap<Segment, String[]>();

    List<FileStatus> files = listStatus(job);
    IndexedFileReader reader = new IndexedFileReader();
    LOG.info("Reading indices");
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
        Segment coverSeg = getCoverSegments(seg, blkLocations);
        coverSegMap.put(seg, coverSeg);
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
            if (coverSegMap.get(seg) == null) {
              Segment coverSeg = getCoverSegments(seg, blkLocations);
              coverSegMap.put(seg, coverSeg);
            }
            if (segLocMap.get(seg) == null) {
              String[] hosts = collectHosts(blkLocations, off, len);
              segLocMap.put(seg, hosts);
            }
          }
        }
      }
    }

    //filtering through the index list and construct Map2Splits

    //To do: this filtering interface is where the restriction of two input is
    //enforced. Maybe use a parser in the future

    splits = filterAndCombine(job, idxList, segmentList, 
                              coverSegMap, segLocMap);
    //StringBuilder sb = new StringBuilder();
    //sb.append("Created splits:\n");
    //for (InputSplit split : splits) {
    //  sb.append(split);
    //  sb.append(",\n");
    //}
    //sb.append("\n");
    //LOG.info(sb.toString());
    //
    //sb.append("Loc map:\n");
    //for (Segment seg: segLocMap.keySet()) {
    //  sb.append(seg + ":\n");
    //  String[] hosts = segLocMap.get(seg);
    //  for (int i = 0; i < hosts.length; ++i) {
    //    sb.append("\t" + hosts[i] + "\n");
    //  }
    //}
    //LOG.info(sb.toString());
    //System.exit(-1);
    return splits;
  }

  private Segment getCoverSegments(Segment seg, BlockLocation[] blkLocations) 
      throws IOException {
    List<BlockLocation> blocks = Arrays.asList(blkLocations);
    List<String> hosts = new ArrayList<String>();
    long start = seg.getOffset();
    long end = start + seg.getLength();
    int idx = 0;

    long coverStart = -1;
    long coverEnd = -1;

    while(idx < blocks.size()) {
      BlockLocation curr = blocks.get(idx);
      long currStart = curr.getOffset();
      long currEnd = currStart + curr.getLength();

      if (currStart <= start && start <= currEnd) {
        coverStart = currStart;
      }

      if (currStart <= end && end <= currEnd) {
        coverEnd = currEnd;
      }

      if (coverStart != -1 && coverEnd != -1) {
        break;
      }
      idx ++;
    }
    return new Segment(seg.getPath(), coverStart, coverEnd - coverStart);
  }

  private String[] collectHosts(BlockLocation[] blkLocations,
                                long off, long len) throws IOException {
    List<BlockLocation> blocks = Arrays.asList(blkLocations);
    List<String> hosts = new ArrayList<String>();
    long start = off;
    long end = start + len;
    int idx = 0;
    while(idx < blocks.size()) {
      BlockLocation curr = blocks.get(idx);
      long currStart = curr.getOffset();
      long currEnd = currStart + curr.getLength();
      if ((currStart <= start && start < currEnd) ||
          (currStart <= end && end < currEnd)) {
        hosts.addAll(Arrays.asList(blkLocations[idx].getHosts()));
      }
      idx ++;
    }
    return hosts.toArray(new String[hosts.size()]);
  }

  protected List<InputSplit> filterAndCombine(
      JobContext job, 
      List<String> idxList, List<Segment[]> segmentList, 
      Map<Segment, Segment> coverSegMap,
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
          Segment[] coverSegs = new Segment[segs.length];
          for (int k = 0; k < segs.length; ++k) {
            coverSegs[k] = coverSegMap.get(segs[k]);
          }
          String[][] hosts = new String[segs.length][];
          for (int k = 0; k < segs.length; ++k) {
              hosts[k] = segLocMap.get(segs[k]);
          }
          Map2Split split = new Map2Split(segs, indices, coverSegs, hosts);
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

  public static void setIndexFilter(
      Job job, Class<? extends Map2Filter> cls) throws IllegalStateException {
    job.getConfiguration().setClass("mapred.map2.input.indexFilter.class",
                                    cls, Map2Filter.class);
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

  public void setFileNameAsIndex(Job job) {
    Configuration conf = job.getConfiguration();
    return conf.setBoolean("mapred.map2.input.fileNameAsIndex", true);
  }
  
  public RecordReader<String[], TrackedSegments> createRecordReader(
      InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new Map2RecordReader();
  }

}
