package org.apache.hadoop.mapred.map2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/** 
 * Map2SplitMetaInfo
 *
 * maintain pairs of split
 */

public class Map2SplitMetaInfo implements Comparable {

  private IndexedSplit[] pair = new IndexedSplit[2];


  public Map2SplitMetaInfo(IndexedSplit s0, IndexedSplit s1) {
    if(s0.compareTo(s1) > 0) {
      pair[0] = s1;
      pair[1] = s0;
    }
    else {
      pair[0] = s0;
      pair[1] = s1;
    }
  }

  public IndexedSplit[] getInfo() {
    return pair;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) return true;
    if (!(to instanceof Map2SplitMetaInfo)) return false;
    Map2SplitMetaInfo that = (Map2SplitMetaInfo) to;
    return (pair[0].equals(that.pair[0]) &&
            pair[1].equals(that.pair[1]));
  }

  @Override
  public int hashCode() {
    return pair[0].hashCode() ^ pair[1].hashCode();
  }

  @Override
  public int compareTo(Object to) {
    if (!(to instanceof Map2SplitMetaInfo)) throw new ClassCastException();
    Map2SplitMetaInfo that = (Map2SplitMetaInfo) to;
    int ret = pair[0].compareTo(that.pair[0]);
    if (ret == 0)
      ret = pair[1].compareTo(that.pair[1]);
    return ret;
  }

  @Override
  public String toString() {
    return pair[0].getIndex() + ", " + pair[1].getIndex();
  }

  public void write(DataOutput out) throws IOException {
    pair[0].write(out);
    pair[1].write(out);
  }

  public void readFields(DataInput in) throws IOException {
    pair[0].readFields(in);
    pair[1].readFields(in);
  }

}
