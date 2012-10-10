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

  private List<Segment[]> tasks;
  private Map2Filter filter;
  private Segment[] inputs;

  public void setFilter(Map2Filter filter) {
    this.filter = filter;
  }

  public void setInputs(List<Segment> inputs) {
    this.inputs = inputs.toArray(new Segment[inputs.size()]);
  }

  public void formatInputs() {
    tasks = new ArrayList<Segment[]>();
    for (int i = 0; i < inputs.length; ++i) {
      for (int j = i; j < inputs.length; ++j) {
        if (filter.accept(inputs[i].toString(), 
                          inputs[j].toString())) {
          Segment[] pair = new Segment[2];
          pair[0] = inputs[i];
          pair[1] = inputs[j];
          tasks.add(pair);
        }
      }
    }
  }

  public List<Segment[]> getTasks() {
    return tasks;
  }

  public int getNumTasks() {
    return tasks.size();
  }
}
