package org.apache.hadoop.map2;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.Segment;
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
      FSDataInputStream in = fs.open(idxPath);
      idxList = new ArrayList<String>();
      segList = new ArrayList<Segment[]>();
      try {
        byte[] idxStart = new byte[IndexingConstants.INDEX_START.length];
        byte[] idxEnd = new byte[IndexingConstants.INDEX_END.length];
        byte[] segStart = new byte[IndexingConstants.SEGMENT_START.length];
        byte[] segEnd = new byte[IndexingConstants.SEGMENT_END.length];
        in.readFully(idxStart);
        if (!Arrays.equals(idxStart, IndexingConstants.INDEX_START))
          throw new IOException("Invalid header on index file");
        while(true) {
          in.readFully(segStart);
          if (!Arrays.equals(segStart, IndexingConstants.SEGMENT_START))
            throw new IOException("Invalid header on index file");
          ArrayList<Segment> subList = new ArrayList<Segment>();
          while(true) {
            long off = in.readLong();
            Segment seg = new Segment();
          }

        }
      }
      catch (EOFException eof) {
      }
      for (int i = 0; i < idxLength; ++i) {
        String idx = Text.readString(in);
        idxList.add(idx);
        int numSegs = in.readInt();
        Segment[] segs = new Segment[numSegs];
        for (int j = 0; j < numSegs; ++j) {
          long off = in.readLong();
          long len = in.readLong();
          segs[j] = new Segment(fs, path, off, len);
        }
        segList.add(segs);
      }
      in.close();
    }
    catch (IOException ioe) {
      LOG.info("Read error on " + idxPath + ": " + 
               StringUtils.stringifyException(ioe));
      throw ioe;
    }
  }

  public List<Segment[]> getSegmentList() {
    return segList;
  }

  public List<String> getIndexList() {
    return idxList;
  }
}
