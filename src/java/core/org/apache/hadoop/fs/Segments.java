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

    public final long remainLen;
    public final Segment segment;

    public CoverInfo(long remainLen, Segment segment) {
      this.remainLen = remainLen;
      this.segment = segment;
    }

    @Override
    public int compareTo(CoverInfo that) {
      if (this == that) return 0;
      if (this.remainLen > that.remainLen) return 1;
      if (this.remainLen == that.remainLen) return 0;
      if (this.remainLen < that.remainLen) return -1;
      return 0;
    }
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(segment.toString() + ", " + remainLen);
      return sb.toString();
    }
  }

  public CoverInfo cover(Segment key) {
    //segments are sorted by path, offset then by length
    int startIdx, endIdx;
    Segment start = new Segment(key.getPath(), 0, 1);
    Segment end = new Segment(key.getPath(), 
                              key.getOffset() + key.getLength() + 1, 1);
    startIdx = Collections.binarySearch(thisList, start);
    startIdx = (startIdx >= 0) ? startIdx : -(startIdx + 1);
    endIdx = Collections.binarySearch(thisList, end);
    endIdx = (endIdx >= 0) ? endIdx : -(endIdx + 1);
    return cover(startIdx, endIdx, key);
  }

  /**
   * Calculate how this colleciton of segments cover the segment.
   */
  public CoverInfo cover(int start, int end, Segment key) {
    if (thisList.size() == 0) return new CoverInfo(key.getLength(), key);
    if (start < 0) start = 0;
    if (end > thisList.size()) end = thisList.size();
    int idx = start;
    long targetStart = key.getOffset();
    long targetEnd = targetStart + key.getLength();
    Segment curr;
    long remainLen = 0;
    //There are six cases when iterating through the list:
    //(1)currStart < currEnd < targetStart < targetEnd
    //(2)currStart < targetStart < currEnd < targetEnd
    //(3)currStart < targetStart < targetEnd < currEnd
    //(4)targetStart < currStart < currEnd < targetEnd
    //(5)targetStart < currStart < targetEnd < currEnd
    //(6)targetStart < targetEnd < currStart < currEnd 
    while(idx < end) {
      curr = thisList.get(idx);
      if (!curr.getPath().equals(key.getPath())) {
        idx ++;
        continue;
      }
      long currStart = curr.getOffset();
      long currEnd = currStart + curr.getLength();
      //case (4) and (5)
      if (targetStart < currStart) {
        remainLen += currStart - targetStart;
        targetStart = currStart;
      }
      //case (3) and (5)
      if (targetEnd < currEnd) {
        targetStart = targetEnd;
        break;
      }
      //case (2) and (4)
      if (targetStart < currEnd) {
        targetStart = currEnd;
      }
      //case (1) do nothing
      //case (6) calculation ends
      if (targetEnd < currStart) break;
      idx ++;
    }

    if (targetEnd > targetStart) remainLen += targetEnd - targetStart;
    return new CoverInfo(remainLen, key);
  }

  /**
   * Calculate how this segments cover each of that segment.
   *
   * Return a list of CoverInfo for each of that segment, null if this is empty.
   */
  public List<CoverInfo> cover(Collection<Segment> that) {
    List<Segment> thatList = new ArrayList<Segment>(that);
    Collections.sort(thatList);
    List<CoverInfo> ret = new ArrayList<CoverInfo>();

    if (thatList.size() == 0) return ret;
    if (thisList.size() == 0) return null;

    //since both lists are sorted, we can limit the search range.
    int startIdx, endIdx;
    Segment first = thisList.get(0);
    Segment last = thisList.get(thisList.size() - 1);
    Segment start = new Segment(first.getPath(), 0, 1);
    Segment end = new Segment(last.getPath(),
                              last.getOffset() + last.getLength() + 1, 1);
    startIdx = Collections.binarySearch(thisList, start);
    startIdx = (startIdx >= 0) ? startIdx : -(startIdx + 1);
    endIdx = Collections.binarySearch(thisList, end);
    endIdx = (endIdx >= 0) ? endIdx : -(endIdx + 1);

    for (int i = startIdx; i < endIdx; ++i) {
      CoverInfo result = cover(thatList.get(i));
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
