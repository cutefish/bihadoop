package org.apache.hadoop.mapred.map2;

import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.fs.Segment;

/**
 * <code>SegmentSplit</code> represents an InputSplit that also has segment
 * information encoded.
 */
public interface SegmentedSplit extends InputSplit {
  
  /**
   * Get the list of segments that compose this input split
   */
  Segment[] getSegments() throws IOException; 
}
