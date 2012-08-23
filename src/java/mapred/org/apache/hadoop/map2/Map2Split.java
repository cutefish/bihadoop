package org.apache.hadoop.mapred.map2;

import org.apache.hadoop.fs.Segment;

/**
 * Multiple segments of files. Returned by {@link
 * Map2InputFormat#getSplits(JobConf, int)} and passed to
 * {@link InputFormat#getRecordReader{InputSplit, Jobconf, Reporter)}.
 */
public class Map2Split implements SegmentedSplit {

  private Segment[] segs;
  private String[] hosts;

  Map2Split() { }

  public Map2Split(Segment[] segs) {
    Map2Split(segs, (String[])null);
  }

  public Map2Split(Segment[] segs, String[] hosts) {
    this.segs = segs;
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
  }

  public String[] getLocations() throws IOException {
    if (this.hosts == null) { 
      return new String[]{};
    }
    else {
      return this.hosts;
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
    }
  }

  public void readFields(DataInput in) throws IOException {
    int num = in.readInt();
    for (int i = 0; i < num; ++i) {
      segs[i].readFields(in);
    }
  }


}
