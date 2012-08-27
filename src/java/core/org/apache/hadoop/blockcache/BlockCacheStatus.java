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

  public BlockCacheStatus() { }

  public BlockCacheStatus(Segments segments,
                          long diskCacheCapacity,
                          long memoryCacheCapacity) {
    this.segments = segments;
    this.diskCacheCapacity = diskCacheCapacity;
    this.memoryCacheCapacity = memoryCacheCapacity;
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
