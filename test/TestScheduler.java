import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.mp2.IndexedSplit;

class TestScheduler {

  class TestJobWrapper {

    private FakeJob job;

    public FakeJob getJob() {
      return job;
    }
  }

  class AllPairJob {

    class AllPairFilter implements Filter {
      public boolean accept() {
        return true;
      }
    }

    AllPairJob(Configuration conf) {
      job = new FakeJob();
      job.setFilter(new AllPairFilter());
      long blockSize = conf.get("split.block.size", 64);
      int numSplit0 = conf.get("allpair.num.split0", 100);
      int numSplit1 = conf.get("allpair.num.split1", 200);
      List<IndexedSplit> splitList = new ArrayList<IndexedSplit>();
      for (int i = 0; i < numSplit0; ++i) {
        splitList.add(IndexedSplit("allpair.split0." + i, blockSize));
      }
      job.setSplit0(splitList);
      splitList = new ArrayList<IndexedSplit>();
      for (int i = 0; i < numSplit0; ++i) {
        splitList.add(IndexedSplit("allpair.split1." + i, blockSize));
      }
      job.setSplit1(splitList);
    }

  }

  public static void main(String[] args) { 
    Configuration conf;
    conf.addResource("test_scheduler.conf");
    Class testClass = conf.getClass("job.class.name", AllPairFilter);
    TestJobWrapper testJob = (TestJobWrapper) testClass.newInstance();
    FakeDistributedSystem system = new FakeDistributedSystem(conf);
    system.submitJob(testJob.getJob());
    system.runJob(testJob.getJob());
  }

}
