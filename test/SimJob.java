import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.Map2Filter;

/* SimJobs.java
 *
 * Collection of job patterns.
 */

public class SimJob {

  private List<Segment[]> tasks;
  private Map2Filter filter;
  private List<Segment> inputs;

  public void setFilter(Map2Filter filter) {
    this.filter = filter;
  }

  public void setInputs(List<Segment> inputs) {
    this.inputs = inputs;
  }

  public List<Segment> getInputs() {
    return inputs;
  }

  public void formatTasks(List<Segment> inputs) {
    tasks = new ArrayList<Segment[]>();
    for (int i = 0; i < inputs.size(); ++i) {
      for (int j = i; j < inputs.size(); ++j) {
        if (filter.accept(inputs.get(i).toString(), 
                          inputs.get(j).toString())) {
          Segment[] pair = new Segment[2];
          pair[0] = inputs.get(i);
          pair[1] = inputs.get(j);
          tasks.add(pair);
        }
      }
    }
  }

  public void formatTasks(List<String> indices, 
                          List<Segment> inputs) {
    tasks = new ArrayList<Segment[]>();
    for (int i = 0; i < inputs.size(); ++i) {
      for (int j = i; j < inputs.size(); ++j) {
        if (filter.accept(indices.get(i), 
                          indices.get(j))) {
          Segment[] pair = new Segment[2];
          pair[0] = inputs.get(i);
          pair[1] = inputs.get(j);
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
