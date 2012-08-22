package org.apache.hadoop.mapred.map2;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility that reads a indexed file .
 *
 * Indexed files are named using the file name plus an suffix .indexa
 */
public class IndexedFileReader {

  private List<Segment[]> segList;
  private List<String> idxList;

  public void readIndexedFile(FileSystem fs, Path path) throws IOException {
    FSDataInputStream in = fs.open(path);
    idxList = new ArrayList<String>();
    segList = new ArrayList<Segment[]>();
    int idxLength = in.readInt();
    for (int i = 0; i < idxLength; ++i) {
      idxList.set(i, Text.readString(in));
      int numSegs = in.readInt();
      Segment[] segs = new Segment[numSegs];
      for (int j = 0; j < numSegs; ++i) {
        segs[j].readField(in);
      }
    }
  }
}
