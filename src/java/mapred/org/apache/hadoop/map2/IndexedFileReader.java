package org.apache.hadoop.map2;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A utility that reads a indexed file .
 *
 * Indexed files are named using the file name plus an suffix .indexa
 */
public class IndexedFileReader {

  private static final Log LOG = LogFactory.getLog(Map2InputFormat.class);

  private List<Segment[]> segList;
  private List<String> idxList;

  /****************************************************************************
   * Indexed File Format:
   * int numIndecies
   * String index
   * int numSegments
   * Long off Long len
   * ...
   */
  public void readIndexedFile(FileSystem fs, Path path) throws IOException {
    Path idxPath = path.suffix(".map2idx");
    try {
      FSDataInputStream in = fs.open(path);
      idxList = new ArrayList<String>();
      segList = new ArrayList<Segment[]>();
      int idxLength = in.readInt();
      for (int i = 0; i < idxLength; ++i) {
        idxList.set(i, Text.readString(in));
        int numSegs = in.readInt();
        Segment[] segs = new Segment[numSegs];
        for (int j = 0; j < numSegs; ++i) {
          long off = in.readLong();
          long len = in.readLong();
          segs[i] = new Segment(fs, path, off, len);
        }
        segList.set(i, segs);
      }
    }
    catch (IOException ioe) {
      LOG.info("Read error on " + idxPath + ": " + 
               StringUtils.stringifyException(ioe));
      throw;
    }
  }

  public List<Segment[]> getSegmentList() {
    return segList;
  }

  public List<String> getIndexList() {
    return idxList;
  }
}
