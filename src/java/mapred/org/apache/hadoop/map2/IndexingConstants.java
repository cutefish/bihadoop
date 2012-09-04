package org.apache.hadoop.map2;

import java.io.UnsupportedEncodingException;

public class IndexingConstants {
  public static final int MARK_LEN = 5;
  public static final int LONG_LEN = 8;
  public static final String utf8 = "UTF-8";
  public static final byte[] IDX_START;
  public static final byte[] IDX_END;
  public static final byte[] SEG_START;

  static {
    try {
      IDX_START = "#IDX#".getBytes("UTF-8");
      IDX_END = "#IDX#".getBytes("UTF-8");
      SEG_START = "#SEG#".getBytes("UTF-8");
    }
    catch (UnsupportedEncodingException uee) {
      throw new IllegalArgumentException("cannot find " + utf8 + "encoding");
    }
  }
}
