package org.apache.hadoop.map2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;


/**
 * Multiple segments of files. Returned by {@link
 * Map2InputFormat#getSplits(JobConf, int)} and passed to
 * {@link InputFormat#getRecordReader{InputSplit, Jobconf, Reporter)}.
 */
public class Map2Split extends SegmentedSplit {

  //A Map2Split can have multiple segments;
  private Segment[] segs;
  private String[] indices;
  //String[] = hosts[i] represents locations for a segment.
  private String[][] hosts; 

  Map2Split() { }

  public Map2Split(Segment[] segs) {
    this(segs, (String[])null, (String[][])null);
  }

  public Map2Split(Segment[] segs, 
                   String[] indices, 
                   String[][] hosts) {
    this.segs = segs;
    this.indices = indices;
    this.hosts = hosts;
  }

  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append("[");
    for (int i = 0; i < segs.length; ++i) {
      ret.append(segs[i].toString());
      ret.append(", ");
    }
    ret.append("]");
    return ret.toString();
  }

  public String[] getLocations() throws IOException {
    if (this.hosts == null) { 
      return new String[]{};
    }
    else {
      List<String> hostList = new ArrayList<String>();
      for (int i = 0; i < hosts.length; ++i) {
        for (int j = 0; j < hosts[i].length; ++j) {
          hostList.add(hosts[i][j]);
        }
      }
      return hostList.toArray(new String[hostList.size()]);
    }
  }

  public Segment[] getSegments() throws IOException {
    if (this.segs == null) { 
      return new Segment[]{};
    }
    else {
      return this.segs;
    }
  }

  public String[] getIndices() throws IOException {
    if (this.segs == null) { 
      return new String[]{};
    }
    else {
      return this.indices;
    }
  }

  public String[][] getHosts() throws IOException {
    if (this.hosts == null) {
      return new String[][]{ };
    }
    else {
      return this.hosts;
    }
  }

  public long getLength() {
    long ret = 0;
    for (Segment seg : segs) {
      ret += seg.getLength();
    }
    return ret;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
        (Map2Split.class,
         new WritableFactory() {
         public Writable newInstance() { return new Map2Split(); }
         });
  }

  //segments are needed for both user mapper and scheduler
  public void write(DataOutput out) throws IOException {
    out.writeInt(segs.length);
    for (int i = 0; i < segs.length; ++i) {
      segs[i].write(out);
      Text.writeString(out, indices[i]);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int num = in.readInt();
    segs = new Segment[num];
    indices = new String[num];
    for (int i = 0; i < num; ++i) {
      segs[i] = new Segment();
      segs[i].readFields(in);
      indices[i] = Text.readString(in);
    }
  }


}
