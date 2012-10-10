import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.Map2Filter;

public abstract class SimJobClient {
  protected Configuration conf;
  protected SimJob job = new SimJob();

  public SimJobClient(Configuration conf) {
    this.conf = conf;
  }

  public abstract void configJob(int iteration);

  public void run() {
    int numIter = conf.getInt("job.client.num.iterations", 1);
    SimDistributedSystem sys = new SimDistributedSystem(conf);

    for (int i = 0; i < numIter; ++i) {
      System.out.format("---------iteration: %d-------\n", i);
      System.out.flush();
      configJob(i);
      sys.createReplicas(job.getInputs());
      sys.runJob(job);
    }
  }
}
