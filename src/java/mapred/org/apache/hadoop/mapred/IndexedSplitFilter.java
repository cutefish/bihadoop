package org.apache.hadoop.mapred.mp2;

public interface IndexedSplitFilter {
  /**
   * Test whether a pair of IndexedSplit forms a map task
   *
   * @param split0 The first split.
   * @param split1 The second split.
   * @return <code>true</code> if and only if the pair forms a map task input.
   */
  boolean accept(IndexedSplit split0, IndexedSplit split1);
}
