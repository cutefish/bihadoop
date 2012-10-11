
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;

public class TestSegment {
  public static void main(String[] args) { 
    try {
      Segment s1 = new Segment(new Path("/data/diagblock.input0"), 64, 64);
      Segment s2 = new Segment(new Path("/data/diagblock.input0"), 64, 64);
      System.out.println("s1 == s2: " + (s1.equals(s2)));
      List<Segment> list = new ArrayList<Segment>();
      for (int i = 0; i < 1024; i += 64) {
        list.add(new Segment(new Path("/data/diagblock.input0"), i, 64));
      }
      int idx = Collections.binarySearch(list, s1);
      System.out.println("idx: " + idx);
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
