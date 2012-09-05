package org.apache.hadoop.map2;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    idxList = new ArrayList<String>();
    segList = new ArrayList<Segment[]>();
    if (path.toString().endsWith(".map2idx")) return;
    Path idxPath = path.suffix(".map2idx");
    FSDataInputStream in = null;
    try {
      in = fs.open(idxPath);
      byte[] mark = new byte[IndexingConstants.MARK_LEN];
      in.readFully(mark);
      if (!Arrays.equals(mark, IndexingConstants.IDX_START))
        throw new IOException("Invalid header on index file");
      while(true) {
        String idx = Text.readString(in);
        idxList.add(idx);
        ArrayList<Segment> segs = new ArrayList<Segment>();
        while(true) {
          in.readFully(mark);
          if (!Arrays.equals(mark, IndexingConstants.SEG_START)) {
            if (!Arrays.equals(mark, IndexingConstants.IDX_END)) {
              throw new IOException("Invalid header on index file");
            }
            else {
              segList.add(segs.toArray(new Segment[segs.size()]));
              in.readFully(mark);
              if (!Arrays.equals(mark, IndexingConstants.IDX_START))
                throw new IOException("Invalid header on index file");
              break;
            }
          }
          long off = in.readLong();
          long len = in.readLong();
          System.out.flush();
          Segment seg = new Segment(fs, path, off, len);
          segs.add(seg);
        }
      }
    }
    catch (EOFException eof) {
      LOG.info("Finish reading index file:" + path);
    }
    catch (IOException ioe) {
      LOG.info("Read error on " + idxPath + ": " + 
               StringUtils.stringifyException(ioe));
      throw ioe;
    }
    finally {
      if (in != null)
        in.close();
    }
  }

  public List<Segment[]> getSegmentList() {
    return segList;
  }

  public List<String> getIndexList() {
    return idxList;
  }
}
