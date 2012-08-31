package org.apache.hadoop.map2;

import org.apache.hadoop.fs.Segment;

/**
 * This is a work around to report progress, we should find a better way to do that.
 */
public class TrackedSegments {
  public volatile float progress = 0;
  public Segment[] segments;
}

