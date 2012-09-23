package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;

/**
 * A collection of segments
 */
public class Segments implements Writable {

  private List<Segment> thisList;

  public Segments() {
    thisList = new ArrayList<Segment>();
  }

  public Segments(Collection<Segment> c) {
    thisList = new ArrayList<Segment>(c);
    Collections.sort(thisList);
  }

  public Segments(Segments s) {
    thisList = new ArrayList<Segment>(s.thisList);
  }

  public Segments(Segment[] s) {
    thisList = new ArrayList<Segment>(Arrays.asList(s));
    Collections.sort(thisList);
  }

  public void add(Segments s) {
    thisList.addAll(s.thisList);
    Collections.sort(thisList);
  }

  public List<Segment> getList() {
    return thisList;
  }

  public int size() {
    return thisList.size();
  }

  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj != null && obj instanceof Segments) {
      Segments that = (Segments)obj;
      if (this.thisList.size() != that.thisList.size())
        return false;
      for (int i = 0; i < thisList.size(); ++i) {
        if (!thisList.get(i).equals(that.thisList.get(i)))
          return false;
      }
      return true;
    }
    return false;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("segments[");
    for (Segment seg : thisList) {
      sb.append(seg.toString() + ", ");
    }
    sb.append("]");
    return sb.toString();
  }

  public static class CoverInfo implements Comparable<CoverInfo> {

    public final long leftSize;
    public final Segment segment;

    public CoverInfo(long leftSize, Segment segment) {
      this.leftSize = leftSize;
      this.segment = segment;
    }

    @Override
    public int compareTo(CoverInfo that) {
      if (this == that) return 0;
      if (this.leftSize > that.leftSize) return 1;
      if (this.leftSize == that.leftSize) return 0;
      if (this.leftSize < that.leftSize) return -1;
      return 0;
    }
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(segment.toString() + ", " + leftSize);
      return sb.toString();
    }
  }

  public CoverInfo cover(Segment key) {
    return cover(0, thisList.size() - 1, key);
  }

  /**
   * Calculate how this colleciton of segments cover the segment.
   *
   * We assume that the segments in the collection does not have very large
   * overlap portions.
   */
  public CoverInfo cover(int hintStart, int hintEnd, Segment key) {
    if (thisList.size() == 0) return new CoverInfo(key.getLength(), key);
    if (hintStart < 0) hintStart = 0;
    if (hintEnd > thisList.size()) hintEnd = thisList.size();
    int idx = hintStart;
    long targetStart = key.getOffset();
    long targetEnd = targetStart + key.getLength();
    Segment curr;
    long leftSize = 0;
    //There are six cases:
    //(1)curr.start > curr.end > targetStart > targetEnd
    //(2)curr.start > targetStart > curr.end > targetEnd
    //(3)curr.start > targetStart > targetEnd > curr.end
    //(4)targetStart > curr.start > curr.end > targetEnd
    //(5)targetStart > curr.start > targetEnd > curr.end
    //(6)targetStart > targetEnd > curr.start > curr.end 
    while(idx < hintEnd) {
      curr = thisList.get(idx);
      if (!curr.getPath().equals(key.getPath())) {
        idx ++;
        continue;
      }
      long currStart = curr.getOffset();
      long currEnd = currStart + curr.getLength();
      //case (6) calculation ends
      if (currStart > targetEnd) break;
      //case (4) and (5)
      if (currStart > targetStart) {
        leftSize += currStart - targetStart;
        targetStart = currStart;
      }
      //case (3) and (5)
      if (currEnd > targetEnd) {
        targetStart = targetEnd;
        break;
      }
      //case (2) and (4)
      if (targetStart <= currEnd) {
        targetStart = currEnd;
      }
      //case (1) do nothing
      idx ++;
    }

    if (targetEnd > targetStart) leftSize += targetEnd - targetStart;
    return new CoverInfo(leftSize, key);
  }

  public List<CoverInfo> cover(Collection<Segment> that) {
    List<Segment> thatList = new ArrayList<Segment>(that);
    Collections.sort(thatList);
    List<CoverInfo> ret = new ArrayList<CoverInfo>();

    if (thatList.size() == 0) return ret;
    if (thisList.size() == 0) return null;

    //since both lists are sorted, we can limit the search range.
    int idx;
    idx = Collections.binarySearch(thisList, thatList.get(0));
    int thisStart = (idx >= 0) ? idx : -(idx + 1) - 1;
    idx = Collections.binarySearch(thisList, thatList.get(thatList.size() - 1));
    int thisEnd = (idx >= 0) ? idx : -(idx + 1) + 1;
    idx = Collections.binarySearch(thatList, thisList.get(0));
    int thatStart = (idx >= 0) ? idx : -(idx + 1) - 1;
    idx = Collections.binarySearch(thatList, thisList.get(thisList.size() - 1));
    int thatEnd = (idx >= 0) ? idx : -(idx + 1) + 1;
    if (thatStart < 0) thatStart = 0;
    if (thatEnd >= thatList.size()) thatEnd = thatList.size() - 1;

    //for (int i = thatStart; i <= thatEnd; ++i) {
    for (int i = 0; i < thatList.size(); ++i) {
      CoverInfo result = cover(thisStart, thisEnd, thatList.get(i));
      ret.add(result);
    }

    Collections.sort(ret);
    return ret;
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
    out.writeInt(thisList.size());
    for (Segment seg : thisList) {
      seg.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    System.out.println(length);
    for (int i = 0; i < length; ++i) {
      Segment seg = new Segment();
      seg.readFields(in);
      thisList.add(seg);
    }
  }

}
