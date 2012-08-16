import java.lang.reflect.Constructor;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.map2.IndexedSplit;
import org.apache.hadoop.mapred.map2.Map2Filter;

class TestScheduler {

  static class TestJobWrapper {

    protected FakeJob job;

    public FakeJob getJob() {
      return job;
    }
  }

  static class AllPairJob extends TestJobWrapper {

    static class AllPairFilter implements Map2Filter {
      public boolean accept(IndexedSplit s0, IndexedSplit s1) {
        return ((s0.getIndex().contains("split0")) &&
                (s1.getIndex().contains("split1")));
      }
    }

    AllPairJob(Configuration conf) {
      job = new FakeJob();
      job.setFilter(new AllPairFilter());
      long blockSize = conf.getInt("split.block.size", 64);
      int numSplit0 = conf.getInt("allpair.num.split0", 100);
      int numSplit1 = conf.getInt("allpair.num.split1", 200);
      List<IndexedSplit> splitList = new ArrayList<IndexedSplit>();
      for (int i = 0; i < numSplit0; ++i) {
        splitList.add(new IndexedSplit("allpair.split0." + i, blockSize));
      }
      for (int i = 0; i < numSplit1; ++i) {
        splitList.add(new IndexedSplit("allpair.split1." + i, blockSize));
      }
      job.setSplits(splitList);
    }

  }

  static class AllPairDiagJob extends TestJobWrapper {

    static class AllPairDiagFilter implements Map2Filter {
      public boolean accept(IndexedSplit s0, IndexedSplit s1) {
        return true;
      }
    }

    AllPairDiagJob(Configuration conf) {
      job = new FakeJob();
      job.setFilter(new AllPairDiagFilter());
      long blockSize = conf.getInt("split.block.size", 64);
      int numSplit0 = conf.getInt("allpairdiag.num.split", 100);
      List<IndexedSplit> splitList = new ArrayList<IndexedSplit>();
      for (int i = 0; i < numSplit0; ++i) {
        splitList.add(new IndexedSplit("allpair.split." + i, blockSize));
      }
      job.setSplits(splitList);
    }

  }
  static class MatMultJob extends TestJobWrapper {

    class MatMultFilter implements Map2Filter {
      public boolean accept(IndexedSplit s0, IndexedSplit s1) {
        return true;
      }
    }

  }

  public static void main(String[] args) { 
    Configuration conf = new Configuration();
    conf.addResource("test_scheduler.conf.xml");
    try {
      Class testClass = conf.getClass("job.class.name", 
                                      Class.forName("TestScheduler$AllPairJob"));
      System.out.println("Job " + testClass.toString());
      Class[] paramType = new Class[1];
      paramType[0] = Configuration.class;
      Constructor ctor = testClass.getDeclaredConstructor(paramType);
      TestJobWrapper testJob = (TestJobWrapper) ctor.newInstance(conf);
      FakeDistributedSystem system = new FakeDistributedSystem(conf);

      system.submitJob(testJob.getJob());
      system.runJob(testJob.getJob());
    }
    catch (Exception e) {
      System.out.println(e);
      System.exit(-1);
    }
  }

}
