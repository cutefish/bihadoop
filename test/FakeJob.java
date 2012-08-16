import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.map2.IndexedSplit;
import org.apache.hadoop.mapred.map2.Map2Filter;

/* FakeJobs.java
 *
 * Collection of job patterns.
 */

public class FakeJob {

  private List<IndexedSplit[]> taskList;
  private Map2Filter filter;
  private IndexedSplit[] splits;

  public void setFilter(Map2Filter filter) {
    this.filter = filter;
  }

  public void setSplits(List<IndexedSplit> splits) {
    this.splits = splits.toArray(new IndexedSplit[splits.size()]);
  }

  public IndexedSplit[] getSplits() {
    return splits;
  }

  public void init() {
    taskList = new ArrayList<IndexedSplit[]>();
    for (int i = 0; i < splits.length; ++i) {
      for (int j = i; j < splits.length; ++j) {
        if (filter.accept(splits[i], splits[j])) {
          IndexedSplit[] splitPair = new IndexedSplit[2];
          splitPair[0] = splits[i];
          splitPair[1] = splits[j];
          taskList.add(splitPair);
        }
      }
    }
  }

  public List<IndexedSplit[]> getTaskList() {
    return taskList;
  }

  public int getNumTasks() {
    return taskList.size();
  }
}
