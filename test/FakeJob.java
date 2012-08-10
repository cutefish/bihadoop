import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.mp2.IndexedSplit;
import org.apache.hadoop.mapred.mp2.IndexedSplitFilter;

/* FakeJobs.java
 *
 * Collection of job patterns.
 */

public class FakeJob {

  private Configuration conf;
  private List<IndexedSplit[]> taskList;
  private IndexedSplit[] split0;
  private IndexedSplit[] split1;
  private IndexedSplitFilter filter;

  public FakeJob(Configuration conf) {
    this.conf = conf;
  }

  public void setSplit0(IndexedSplit[] split0) {
    this.split0 = split0;
  }

  public void setSplit0(ArrayList split0) {
    this.split0 = split0.toArray(new IndexedSplit[split0.size()]);
  }

  public void setSplit1(IndexedSplit[] split1) {
    this.split1 = split1;
  }

  public void setSplit1(ArrayList split1) {
    this.split1 = split1.toArray(new IndexedSplit[split0.size()]);
  }

  public void init() {
    filter = conf.get("job.filter.class");
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
