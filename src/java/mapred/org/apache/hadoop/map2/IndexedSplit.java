package org.apache.hadoop.mapred.map2;

import org.apache.hadoop.io.Text;

/* IndexedSplit
 * A split that can be identified by an index.
 */

public class IndexedSplit implements Comparable {

  private String index;
  private long size;


  IndexedSplit(String index, long size) {
    this.index = index;
    this.size = size;
  }

  public String getIndex() {
    return index;
  }

  public long size() {
    return size;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) return true;
    if (!(to instanceof IndexedSplit)) return false;
    return (index.equals(((indexedSplit)to).getIndex()));
  }

  @Override
  public int hashCode() {
    return index.hashCode();
  }

  @Override
  public int compareTo(Object to) {
    if (!(to instanceof IndexedSplit)) throws new ClassCastException;
    return (index.compareTo(((indexedSplit)to).getIndex()));
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, index);
  }

  public void readFields(DataInput in) throws IOException {
    index = Text.readString(in);
  }

}
