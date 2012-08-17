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

    public void update() {
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

  static class MatMatMultJob extends TestJobWrapper {

    class MatMatMultFilter implements Map2Filter {
      public boolean accept(IndexedSplit s0, IndexedSplit s1) {
        String[] nameArray0 = s0.getIndex().split("\\.");
        String[] nameArray1 = s1.getIndex().split("\\.");
        String matName0 = nameArray0[2];
        int row0 = Integer.parseInt(nameArray0[3]);
        int col0 = Integer.parseInt(nameArray0[4]);
        String matName1 = nameArray1[2];
        int row1 = Integer.parseInt(nameArray1[3]);
        int col1 = Integer.parseInt(nameArray1[4]);
        if ((matName0.equals("A")) && 
            (matName1.equals("B")) &&
            (col0 == row1)) {
          return true;
        }
        return false;
      }
    }

    MatMatMultJob(Configuration conf) {
      job = new FakeJob();
      job.setFilter(new MatMatMultFilter());
      long blockSize = conf.getInt("split.block.size", 64);
      int numSplitARow = conf.getInt("matmatmult.num.split.A.row", 4);
      int numSplitACol = conf.getInt("matmatmult.num.split.A.col", 8);
      int numSplitBRow = conf.getInt("matmatmult.num.split.B.row", 8);
      int numSplitBCol = conf.getInt("matmatmult.num.split.B.col", 8);
      List<IndexedSplit> splitList = new ArrayList<IndexedSplit>();
      System.out.println("A: " + numSplitARow + " * " + numSplitACol);
      System.out.println("B: " + numSplitBRow + " * " + numSplitBCol);
      for (int i = 0; i < numSplitARow; ++i) {
        for (int j = 0; j < numSplitACol; ++j) {
          splitList.add(new IndexedSplit("matmatmul.split." + 
                                         "A." + i + "." + j, blockSize));
        }
      }
      for (int i = 0; i < numSplitBRow; ++i) {
        for (int j = 0; j < numSplitBCol; ++j) {
          splitList.add(new IndexedSplit("matmatmul.split." + 
                                         "B." + i + "." + j, blockSize));
        }
      }
      job.setSplits(splitList);
    }


  }

  static class MatVecMultJob extends TestJobWrapper {

    private int numIteration = 0;
    long blockSize;
    int numSplitMRow;
    int numSplitMCol;
    int numSplitVRow;

    class MatVecMultFilter implements Map2Filter {
      public boolean accept(IndexedSplit s0, IndexedSplit s1) {
        String[] nameArray0 = s0.getIndex().split("\\.");
        String[] nameArray1 = s1.getIndex().split("\\.");
        String matName0 = nameArray0[2];
        int row0 = Integer.parseInt(nameArray0[3]);
        int col0;
        if (nameArray0.length >=5) 
          col0 = Integer.parseInt(nameArray0[4]);
        else
          col0 = -1;
        String matName1 = nameArray1[2];
        int row1 = Integer.parseInt(nameArray1[3]);
        if ((matName0.equals("M")) && 
            (matName1.startsWith("V")) &&
            (col0 == row1)) {
          return true;
        }
        return false;
      }
    }

    MatVecMultJob(Configuration conf) {
      job = new FakeJob();
      job.setFilter(new MatVecMultFilter());
      blockSize = conf.getInt("split.block.size", 64);
      numSplitMRow = conf.getInt("matmatmult.num.split.A.row", 4);
      numSplitMCol = conf.getInt("matmatmult.num.split.A.col", 8);
      numSplitVRow = conf.getInt("matmatmult.num.split.B.row", 8);
      List<IndexedSplit> splitList = new ArrayList<IndexedSplit>();
      for (int i = 0; i < numSplitMRow; ++i) {
        for (int j = 0; j < numSplitMCol; ++j) {
          splitList.add(new IndexedSplit("matvecmul.split." + 
                                         "M." + i + "." + j, blockSize));
        }
      }
      for (int i = 0; i < numSplitVRow; ++i) {
        splitList.add(new IndexedSplit("matmatmul.split." + 
                                       "V" + numIteration + 
                                       "." + i, blockSize));
      }
      job.setSplits(splitList);
    }

    @Override
    public void update() {
      numIteration ++;
      List<IndexedSplit> splitList = new ArrayList<IndexedSplit>();
      for (int i = 0; i < numSplitMRow; ++i) {
        for (int j = 0; j < numSplitMCol; ++j) {
          splitList.add(new IndexedSplit("matvecmul.split." + 
                                         "M." + i + "." + j, blockSize));
        }
      }
      for (int i = 0; i < numSplitVRow; ++i) {
        splitList.add(new IndexedSplit("matmatmul.split." + 
                                       "V" + numIteration + 
                                       "." + i, blockSize));
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
      FakeDistributedSystem system = new FakeDistributedSystem(conf);

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
