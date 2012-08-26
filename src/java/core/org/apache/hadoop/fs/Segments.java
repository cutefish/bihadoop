package org.apache.hadoop.fs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A collection of segments
 */
public class Segments implements Writable {

  private List<Segment> thisList;

  public Segments() {
    segments = null;
  }

  public Segments(Collection<Segment> c) {
    thisList = new ArrayList<Segment>(c);
    Collections.sort(thisList);
  }

  public Segments(Segments s) {
    thisList = new ArrayList<Segment>(s.thisList);
  }

  public void add(Segments s) {
    thisList.addAll(s.thisList);
    Collections.sort(thisList);
  }

  public List<Segment> getList() {
    return thisList;
  }

  public static class CoverInfo implements Comparable<CoverInfo> {

    public final long leftSize;
    public final Segment segment;

    public CoverResult(long leftSize, Segment segment) {
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
    if (hintStart < 0) hintStart = 0;
    if (hintEnd > thisList.size()) hintEnd = thisList.size();
    int idx = Collections.binarySearch(
        thisList.subList(hintStart, hintEnd), key);
    idx = (idx >= 0) ? idx : -(idx + 1);
    if (idx > 0) idx --;
    long targetStart = key.start;
    long targetEnd = key.end;
    Segment curr;
    long leftSize = 0;
    //There are six cases:
    //(1)curr.start > curr.end > targetStart > targetEnd
    //(2)curr.start > targetStart > curr.end > targetEnd
    //(3)curr.start > targetStart > targetEnd > curr.end
    //(4)targetStart > curr..start > curr.end > targetEnd
    //(5)targetStart > curr.start > targetEnd > curr.end
    //(6)targetStart > targetEnd > curr.start > curr.end 
    while(true) {
      if (idx >= thisList.size()) break;
      curr = thisList.get(idx);
      //case (6) calculation ends
      if (curr.start > targetEnd) break;
      //case (4) and (5)
      if (curr.start > targetStart) {
        leftSize += curr.start - targetStart;
        targetStart = curr.start;
      }
      //case (3) and (5)
      if (curr.end > targetEnd) {
        targetStart = targetEnd;
        break;
      }
      //case (2) and (4)
      if (targetStart <= curr.end) {
        targetStart = curr.end;
      }
      //case (1) do nothing
      idx ++;
    }

    if (targetEnd > targetStart) leftSize += targetEnd - targetStart;
    return new CoverInfo(leftSize, key);
  }

  public List<CoverInfo> cover(Collection<Segment> that) {
    List<Segment> thatList = new ArrayList<Range>(that);
    Collections.sort(thatList);
    List<CoverInfo> ret = new ArrayList<CoverResult>();

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

    for (int i = thatStart; i <= thatEnd; ++i) {
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
    out.writeInt(segments.size());
    for (seg : segments) {
      seg.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    segments = new ArrayList<Segment>(length);
    for (int i = 0; i < length; ++i) {
      segments.get(i).readFields(in);
    }
  }

}
