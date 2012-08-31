package org.apache.hadoop.map2;

public class IndexingConstants {
  public static final byte[] INDEX_START = "IDX-S".getBytes("UTF-8");
  public static final byte[] INDEX_END = "#IDX-E".getBytes("UTF-8");
  public static final byte[] SEGMENT_START = "SEG-S".getBytes("UTF-8");
  public static final byte[] SEGMENT_END = "SEG-E".getBytes("UTF-8");
}
