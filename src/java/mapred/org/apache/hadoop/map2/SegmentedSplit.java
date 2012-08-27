package org.apache.hadoop.map2;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.io.Writable;

/**
 * <code>SegmentSplit</code> represents an InputSplit that also has segment
 * information encoded.
 */
public abstract class SegmentedSplit extends InputSplit implements Writable {
  
  /**
   * Get the list of segments that compose this input split
   */
  abstract Segment[] getSegments() throws IOException; 
}
