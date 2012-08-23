package org.apache.hadoop.fs;

import java.net.URI;

/**
 * Unique Identity of a contiguous block of data.
 *
 * It is the responsibility of caller of constructor to construct a qualified
 * Segment.
 *
 */
public class Segment implements Writable {
  private Path path;
  private long off;
  private long len;

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
    URI uri = fs.getUri();
    Segment(uri.getScheme(), uri.getAuthority(), path.toString(), off, len);
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
    return path.toString() + "#" + off + "-" + len;
  }

  static boolean isEqual(Object a, Object b) {
    return a == b || (a != null & a.equals(b));
  }

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

