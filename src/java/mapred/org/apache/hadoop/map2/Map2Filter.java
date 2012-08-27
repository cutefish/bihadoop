package org.apache.hadoop.map2;

public interface Map2Filter {
  /**
   * Return true if accept this IndexedSplit pair
   */
  public boolean accept(String idx0, String idx1);
}
