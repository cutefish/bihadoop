package org.apache.hadoop.map2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.fs.Segment;

/**
 * String[] indices as key, Segment[] segments as value
 * one split is also one record
 */
public class Map2RecordReader extends RecordReader<String[], TrackedSegments> {
  private static final Log LOG = LogFactory.getLog(Map2RecordReader.class);

  private boolean firstIteration;
  private String[] indices;
  private TrackedSegments trackedSegs = new TrackedSegments();

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    LOG.info("Initializing");
    Map2Split split = (Map2Split) genericSplit;
    firstIteration = true;
    indices = split.getIndices();
    trackedSegs.segments = split.getSegments();
    StringBuilder sb = new StringBuilder();
    sb.append("indices: [");
    for (String index : indices) {
      sb.append(index + ", ");
    }
    sb.append("]\n");
    sb.append("segments: [");
    for (Segment seg : trackedSegs.segments) {
      sb.append("" + seg + ", ");
    }
    sb.append("]\n");
    LOG.info(sb.toString());
  }
  
  public boolean nextKeyValue() throws IOException {
    if (firstIteration) {
      firstIteration = false;
      return true;
    }
    return false;
  }

  @Override
  public String[] getCurrentKey() {
    return indices;
  }

  @Override
  public TrackedSegments getCurrentValue() {
    return trackedSegs;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    return trackedSegs.progress;
  }
  
  public synchronized void close() throws IOException {
  }
}
