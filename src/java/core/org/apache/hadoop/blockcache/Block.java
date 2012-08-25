package org.apache.hadoop.blockcache;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Path;

/**
 * Block Information for the protocol, adding extra information to the segment.
 */
public class Block extends Segment implements Writable {
  boolean useReplica;
  String localPath;

  public Block() {
    super();
    useReplica = false;
    localPath = null;
  }

  public Block(Path path, long len, long off, 
               boolean useReplica, String localPath) {
    super(path, len, off);
    this.useReplica = useReplica;
    this.localPath = localPath;
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
    return super;
  }

  public String toString() {
    return super.toString();
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
    super.write(out);
    out.writeBoolean(useReplica);
    Text.writeString(out, localPath);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    useReplica = in.readBoolean();
    localPath = Text.readString(in);
  }

}
