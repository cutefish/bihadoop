package org.apache.hadoop.fs;

/**
 * Writable array of segments for RPC
 */
public class Segments implements Writable {
  private Segment[] segments;

  public Segments() {
    segments = null;
  }

  public Segments(Segment[] segs) {
    this.segmnets = segs;
  }

  public Segment[] get() {
    return segments;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
        (Segments.class,
         new WritableFactory() {
         public Writable newInstance() { return new Segments(); }
         });
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(segments.length);
    for (seg : segments) {
      seg.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    segments = new Segment[length]();
    for (int i = 0; i < length; ++i) {
      segments[i].readFields(in);
    }
  }

}
