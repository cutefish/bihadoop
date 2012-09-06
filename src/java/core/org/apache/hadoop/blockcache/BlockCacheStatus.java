package org.apache.hadoop.blockcache;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;

import org.apache.hadoop.fs.Segments;

/**
 * Current BlockCache status.
 */
public class BlockCacheStatus implements Writable {
  public Segments segments;
  public long diskCacheCapacity;
  public long memoryCacheCapacity;

  public BlockCacheStatus() { 
    segments = new Segments();
  }

  public BlockCacheStatus(Segments segments,
                          long diskCacheCapacity,
                          long memoryCacheCapacity) {
    this.segments = segments;
    this.diskCacheCapacity = diskCacheCapacity;
    this.memoryCacheCapacity = memoryCacheCapacity;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(segments.toString() + ", ");
    sb.append("diskCap: " + diskCacheCapacity + ", ");
    sb.append("memCap: " + memoryCacheCapacity + ", ");
    return sb.toString();
  }

  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj != null && obj instanceof BlockCacheStatus) {
        BlockCacheStatus that = (BlockCacheStatus)obj;
        if (this.diskCacheCapacity != that.diskCacheCapacity)
          return false;
        if (this.memoryCacheCapacity != that.memoryCacheCapacity)
          return false;
        if (!this.segments.equals(that.segments))
          return false;
        return true;
    }
    return false;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
        (BlockCacheStatus.class,
         new WritableFactory() {
         public Writable newInstance() { return new BlockCacheStatus(); }
         });
  }

  public void write(DataOutput out) throws IOException {
    segments.write(out);
    out.writeLong(diskCacheCapacity);
    out.writeLong(memoryCacheCapacity);
  }

  public void readFields(DataInput in) throws IOException {
    segments.readFields(in);
    diskCacheCapacity = in.readLong();
    memoryCacheCapacity = in.readLong();
  }

}
