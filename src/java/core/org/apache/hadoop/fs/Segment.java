package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;

/**
 * Unique Identity of a contiguous block of data.
 *
 * It is the responsibility of caller of constructor to construct a qualified
 * Segment.
 *
 */
public class Segment implements Writable, Comparable<Segment> {
  protected Path path;
  protected long off;
  protected long len;

  public Segment() {
    path = null;
    off = 0;
    len = 0;
  }

  public Segment(String scheme, String auth, String path, long off, long len) {
    this.path = new Path(scheme, auth, path);
    this.off = off;
    this.len = len;
  }

  public Segment(Path path, long off, long len) {
    this.path = path;
    this.off = off;
    this.len = len;
  }

  public Segment(FileSystem fs, Path path, long off, long len) {
    this(fs.getUri().getScheme(), fs.getUri().getAuthority(), 
         path.toString(), off, len);
  }

  public Segment(Segment that) {
    this.path = that.path;
    this.off = that.off;
    this.len = that.len;
  }

  public Path getPath() {
    return path;
  }

  public long getOffset() {
    return off;
  }

  public long getLength() {
    return len;
  }

  public String toString() {
    return path.toString() + "#EOP#" + off + "-" + len;
  }

  static public Segment parseSegment(String s) {
    String[] split = s.split("#EOP#");
    if (split.length != 2) 
      throw new IllegalArgumentException("Invalid segment string: " + s);
    Path p = new Path(split[0]);
    split = split[1].split("-");
    if (split.length != 2) 
      throw new IllegalArgumentException("Invalid segment string: " + s);
    long off = Long.parseLong(split[0]);
    long len = Long.parseLong(split[1]);
    return new Segment(p, off, len);
  }

  static boolean isEqual(Object a, Object b) {
    return a == b || (a != null && a.equals(b));
  }

  @Override
  public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj != null && obj instanceof Segment) {
        Segment that = (Segment)obj;
        return isEqual(this.path, that.path)
            && (this.off == that.off)
            && (this.len == that.len);
      }
      return false;
  }

  @Override
  public int hashCode() {
    return path.hashCode() ^ (int)off ^ (int)len;
  }

  @Override
  public int compareTo(Segment that) {
    if (this == that) return 0;
    int ret = this.path.compareTo(that.path);
    if (ret != 0) return ret;
    if (this.off > that.off) return 1;
    if (this.off < that.off) return -1;
    if (this.len > that.len) return 1;
    if (this.len < that.len) return -1;
    return 0;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
        (Segment.class,
         new WritableFactory() {
         public Writable newInstance() { return new Segment(); }
         });
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, path.toString());
    out.writeLong(off);
    out.writeLong(len);
  }

  public void readFields(DataInput in) throws IOException {
    path = new Path(Text.readString(in));
    off = in.readLong();
    len = in.readLong();
  }

}

