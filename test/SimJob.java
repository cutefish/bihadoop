import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.Map2Filter;

/* SimJobs.java
 *
 * Collection of job patterns.
 */

public class SimJob {

  private List<Segment[]> taskList;
  private Map2Filter filter;
  private Segment[] splits;

  public void setFilter(Map2Filter filter) {
    this.filter = filter;
  }

  public void setSplits(List<Segment> splits) {
    this.splits = splits.toArray(new Segment[splits.size()]);
  }

  public Segment[] getSplits() {
    return splits;
  }

  public void init() {
    taskList = new ArrayList<Segment[]>();
    for (int i = 0; i < splits.length; ++i) {
      for (int j = i; j < splits.length; ++j) {
        if (filter.accept(splits[i].toString(), 
                          splits[j].toString())) {
          Segment[] splitPair = new Segment[2];
          splitPair[0] = splits[i];
          splitPair[1] = splits[j];
          taskList.add(splitPair);
        }
      }
    }
  }

  public List<Segment[]> getTaskList() {
    return taskList;
  }

  public int getNumTasks() {
    return taskList.size();
  }
}
