package org.apache.hadoop.blockcache;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Path;

/**
 * Block Information for the protocol
 */
public class Block implements Writable {
  private Segment seg;
  boolean useReplica;
  String localPath;

  public Block() {
    seg = null;
    useReplica = false;
    localPath = null;
  }

  public Block(Segment seg, boolean useReplica, String localPath) {
    this.seg = seg;
    this.useReplica = useReplica;
    this.localPath = localPath;
  }

  public Path getPath() {
    return seg.getPath();
  }

  public long getStartOffset() {
    return seg.getOffset();
  }

  public long getBlockLength() {
    return seg.getLength();
  }

  public boolean shouldUseReplica() {
    return useReplica;
  }

  public String getLocalPath() {
    return localPath;
  }

  public void setLocalPath(String localPath) {
    this.localPath = localPath;
  }

  public Segment getSegment() {
    return seg;
  }

  public String toString() {
    return seg.toString();
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
        (Block.class,
         new WritableFactory() {
         public Writable newInstance() { return new Block(); }
         });
  }

  public void write(DataOutput out) throws IOException {
    seg.write(out);
    out.writeBoolean(useReplica);
    Text.writeString(out, localPath);
  }

  public void readFields(DataInput in) throws IOException {
    seg.readFields(in);
    useReplica = in.readBoolean();
    localPath = Text.readString(in);
  }

}
