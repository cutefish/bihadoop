import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.mp2.IndexedSplit;
import org.apache.hadoop.mapred.mp2.IndexedSplitFilter;

/* FakeJobs.java
 *
 * Collection of job patterns.
 */

public class FakeJob {

  private List<IndexedSplit[]> taskList;
  final private IndexedSplit[] split0;
  final private IndexedSplit[] split1;
  private Filter filter;

  static public interface Filter {
    public boolean accept(IndexedSplit s0, IndexedSplit s1) {
    }
  }

  public void setFilter(Filter f) {
    filter = f;
  }

  public void setSplit0(List split0) {
    this.split0 = split0.toArray(new IndexedSplit[split0.size()]);
  }

  public void setSplit1(List split1) {
    this.split1 = split1.toArray(new IndexedSplit[split0.size()]);
  }

  public IndexedSplit[] getSplit0() {
    return split0;
  }

  public IndexedSplit[] getSplit1() {
    return split1;
  }

  public void init() {
    taskList = new List<IndexedSplit[]>();
    for (IndexedSplit s0 : split0) {
      for (IndexedSplit s1: split1) {
        if (filter.accept(s0, s1)) {
          IndexedSplit[] splitPair = new IndexedSplit[2];
          splitPair[0] = s0;
          splitPair[1] = s1;
          taskList.add(splitPair);
        }
      }
    }
  }

  public List<IndexedSplit[]> getTaskList() {
    return taskList;
  }
}
