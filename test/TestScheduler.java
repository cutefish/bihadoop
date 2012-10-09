import java.lang.reflect.Constructor;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.Map2Filter;

class TestScheduler {

  static class TestJobWrapper {

    protected SimJob job;

    public SimJob getJob() {
      return job;
    }

    public void update() {
    }
  }

  static class AllPairJob extends TestJobWrapper {

    static class AllPairFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        return ((s0.contains("split0")) &&
                (s1.contains("split1")));
      }
    }

    AllPairJob(Configuration conf) {
      job = new SimJob();
      job.setFilter(new AllPairFilter());
      long blockSize = conf.getInt("split.block.size", 64);
      int numSplit0 = conf.getInt("allpair.num.split0", 100);
      int numSplit1 = conf.getInt("allpair.num.split1", 200);
      List<Segment> splitList = new ArrayList<Segment>();
      for (int i = 0; i < numSplit0; ++i) {
        splitList.add(
            new Segment(new Path("allpair.split0"), i*blockSize, blockSize));
      }
      for (int i = 0; i < numSplit1; ++i) {
        splitList.add(
            new Segment(new Path("allpair.split1"), i*blockSize, blockSize));
      }
      job.setSplits(splitList);
    }

  }

  static class AllPairDiagJob extends TestJobWrapper {

    static class AllPairDiagFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        return true;
      }
    }

    AllPairDiagJob(Configuration conf) {
      job = new SimJob();
      job.setFilter(new AllPairDiagFilter());
      long blockSize = conf.getInt("split.block.size", 64);
      int numSplit0 = conf.getInt("allpairdiag.num.split", 100);
      List<Segment> splitList = new ArrayList<Segment>();
      for (int i = 0; i < numSplit0; ++i) {
        splitList.add(
            new Segment(new Path("allpair.split"), i*blockSize, blockSize));
      }
      job.setSplits(splitList);
    }

  }

  static class MatMatMultJob extends TestJobWrapper {

    protected int numARowBlock;
    protected int numAColBlock;
    protected int numBRowBlock;
    protected int numBColBlock;

    public class MatMatMultFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        Segment seg0 = Segment.parseSegment(s0);
        Segment seg1 = Segment.parseSegment(s1);
        String path0 = seg0.getPath().toString();
        String path1 = seg1.getPath().toString();
        long off0 = seg0.getOffset();
        long off1 = seg1.getOffset();
        long len = seg0.getLength();
        int col0 = (int) (off0 / len % numARowBlock);
        int row1 = (int) (off1 / len / numBRowBlock);
        if ((path0.contains("A")) && 
            (path1.contains("B")) &&
            (col0 == row1)) {
          return true;
        }
        return false;
      }
    }

    MatMatMultJob(Configuration conf) {
      job = new SimJob();
      job.setFilter(new MatMatMultFilter());
      long blockSize = conf.getInt("split.block.size", 64);
      numARowBlock = conf.getInt("matmatmult.num.split.A.row", 4);
      numAColBlock = conf.getInt("matmatmult.num.split.A.col", 8);
      numBRowBlock = conf.getInt("matmatmult.num.split.B.row", 8);
      numBColBlock = conf.getInt("matmatmult.num.split.B.col", 8);
      List<Segment> splitList = new ArrayList<Segment>();
      System.out.println("A: " + numARowBlock + " * " + numAColBlock);
      System.out.println("B: " + numBRowBlock + " * " + numBColBlock);
      for (int i = 0; i < numARowBlock; ++i) {
        for (int j = 0; j < numAColBlock; ++j) {
          String name = "mm.A.row" + i + ".col" + j;
          long off = (i * numARowBlock + j) * blockSize;
          long len = blockSize;
          splitList.add(new Segment(new Path(name), off, len));
        }
      }
      for (int i = 0; i < numBRowBlock; ++i) {
        for (int j = 0; j < numBColBlock; ++j) {
          String name = "mm.B.row" + i + ".col" + j;
          long off = (i * numARowBlock + j) * blockSize;
          long len = blockSize;
          splitList.add(new Segment(new Path(name), off, len));
        }
      }
      job.setSplits(splitList);
    }
  }

  static class MatVecMultJob extends MatMatMultJob {

    private int numIteration = 0;
    private int blockSize = 0;

    MatVecMultJob(Configuration conf) {
      super(conf);
      job = new SimJob();
      job.setFilter(new MatMatMultFilter());
      blockSize = conf.getInt("split.block.size", 64);
      numARowBlock = conf.getInt("matmatmult.num.split.A.row", 4);
      numAColBlock = conf.getInt("matmatmult.num.split.A.col", 8);
      numBRowBlock = conf.getInt("matmatmult.num.split.B.row", 8);
      List<Segment> splitList = new ArrayList<Segment>();
      for (int i = 0; i < numARowBlock; ++i) {
        for (int j = 0; j < numAColBlock; ++j) {
          String name = "mm.A.row" + i + ".col" + j;
          long off = (i * numARowBlock + j) * blockSize;
          long len = blockSize;
          splitList.add(new Segment(new Path(name), off, len));
        }
      }
      for (int i = 0; i < numBRowBlock; ++i) {
        for (int j = 0; j < 1; ++j) {
          String name = "mm.B." + numIteration + ".row" + i + ".col" + j;
          long off = (i * numARowBlock + j) * blockSize;
          long len = blockSize;
          splitList.add(new Segment(new Path(name), off, len));
        }
      }
      job.setSplits(splitList);
    }

    @Override
    public void update() {
      numIteration ++;
      List<Segment> splitList = new ArrayList<Segment>();
      for (int i = 0; i < numARowBlock; ++i) {
        for (int j = 0; j < numAColBlock; ++j) {
          String name = "mm.A.row" + i + ".col" + j;
          long off = (i * numARowBlock + j) * blockSize;
          long len = blockSize;
          splitList.add(new Segment(new Path(name), off, len));
        }
      }
      for (int i = 0; i < numBRowBlock; ++i) {
        for (int j = 0; j < 1; ++j) {
          String name = "mm.B." + numIteration + ".row" + i + ".col" + j;
          long off = (i * numARowBlock + j) * blockSize;
          long len = blockSize;
          splitList.add(new Segment(new Path(name), off, len));
        }
      }
      job.setSplits(splitList);
    }

  }
  public static void main(String[] args) { 
    Configuration conf = new Configuration();
    conf.addResource("test_scheduler.conf.xml");
    int numIterations = conf.getInt("job.num.iterations", 1);
    try {
      Class testClass = conf.getClass("job.class.name", 
                                      Class.forName("TestScheduler$AllPairJob"));
      System.out.println("Job " + testClass.toString());
      Class[] paramType = new Class[1];
      paramType[0] = Configuration.class;
      Constructor ctor = testClass.getDeclaredConstructor(paramType);
      TestJobWrapper testJob = (TestJobWrapper) ctor.newInstance(conf);
      SimDistributedSystem system = new SimDistributedSystem(conf);

      for (int i = 0; i < numIterations; ++i) {
        System.out.format("---------iteration: %d-------\n", i);
        System.out.flush();
        system.submitJob(testJob.getJob());
        system.runJob(testJob.getJob());
        testJob.update();
      }
    }
    catch (Exception e) {
      System.out.println(e);
      System.exit(-1);
    }
  }

}
