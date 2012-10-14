import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.map2.Map2Filter;

public class TestScheduler {

  static public List<Segment> getSplits(
      String pathName, long length, long splitLen, float rf) {
    Random r = new Random();
    List<Segment> ret = new ArrayList<Segment>();
    long start, end;
    int num = (int)(length / splitLen);
    start = end = 0;
    int count = 0;
    while(true) {
      if (start >= length) break;
      long rLen = (long)(splitLen * rf);
      rLen = (rLen == 0) ? 1 : rLen;
      long thisLen = splitLen - rLen + 2 * r.nextInt((int)rLen);
      end = start + thisLen;
      count ++;
      if ((end > length) || (count == num)) {
        end = length;
        thisLen = end - start;
      }
      ret.add(new Segment(new Path(pathName), start, thisLen));
      start = end;
    }
    return ret;
  }

  static class AllPair extends SimJobClient {

    static class AllPairFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        return ((s0.contains("input0")) &&
                (s1.contains("input1")));
      }
    }

    public AllPair(Configuration conf) {
      super(conf);
      job.setFilter(new AllPairFilter());
    }

    public void configJob(int iteration) {
      long input0Len = conf.getLong("allpair.input0.length", 1024 * 10);
      long input1Len = conf.getLong("allpair.input1.length", 1024 * 10);
      long split0Len = conf.getLong("allpair.split0.length", 128);
      long split1Len = conf.getLong("allpair.split1.length", 128);
      float randFactor = conf.getFloat("job.split.length.rand", 0.1f);
      job.setInputs(
          Arrays.asList(
              new Segment(new Path("/data/allpair.input0"), 0, input0Len),
              new Segment(new Path("/data/allpair.input1"), 0, input1Len)));
      List<Segment> inputs = new ArrayList<Segment>();
      inputs.addAll(getSplits("/data/allpair.input0", input0Len,
                              split0Len, randFactor));
      inputs.addAll(getSplits("/data/allpair.input1", input1Len,
                              split1Len, randFactor));
      job.formatTasks(inputs);
    }
  }

  static class HalfAllPair extends SimJobClient {

    static class HalfAllPairFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        return true;
      }
    }

    public HalfAllPair(Configuration conf) {
      super(conf);
      job.setFilter(new HalfAllPairFilter());
    }

    public void configJob(int iteration) {
      long inputLen = conf.getLong("halfallpair.input0.length", 1024 * 10);
      long splitLen = conf.getLong("halfallpair.split0.length", 128);
      float randFactor = conf.getFloat("job.split.length.rand", 0.1f);
      job.setInputs(
          Arrays.asList(
              new Segment(new Path("/data/halfallpair.input"), 0, inputLen)));
      List<Segment> inputs = new ArrayList<Segment>();
      inputs.addAll(getSplits("/data/halfallpair.input", inputLen,
                              splitLen, randFactor));
      job.formatTasks(inputs);
    }
  }

  static class DiagBlock extends SimJobClient {

    protected List<Segment> staticSplits = null;

    /**
     * Filter to form a diag block matrix.
     *
     * input0 and input1 resides in the same block is an all pair.
     */
    public class BlockFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        if (!s0.contains("input0")) return false;
        if (!s1.contains("input1")) return false;
        try {
          int blockId0 = Integer.parseInt(s0.split("_")[1]);
          int blockId1 = Integer.parseInt(s1.split("_")[1]);
          if (blockId0 == blockId1) return true;
        }
        catch (Exception e) {
          return false;
        }
        return false;
      }
    }

    public DiagBlock(Configuration conf) {
      super(conf);
      job.setFilter(new BlockFilter());
    }

    public void configJob(int iteration) {
      long input0Len = conf.getLong("diagblock.input0.length", 1024 * 10);
      long input1Len = conf.getLong("diagblock.input1.length", 1024 * 10);
      long split0Len = conf.getLong("diagblock.split0.length", 128);
      long split1Len = conf.getLong("diagblock.split1.length", 128);
      int numBlocks = conf.getInt("diagblock.num.blocks", 16);
      float randFactor = conf.getFloat("job.split.length.rand", 0.1f);
      job.setInputs(
          Arrays.asList(
              new Segment(new Path("/data/diagblock.input0"), 0, input0Len),
              new Segment(new Path("/data/diagblock.input1" + iteration), 
                          0, input1Len)));
      List<Segment> inputs = new ArrayList<Segment>();
      List<String> indices = new ArrayList<String>();
      if ((!conf.getBoolean("job.has.static", true))) {
        inputs.addAll(getSplits("/data/diagblock.input0", input0Len,
                                split0Len, randFactor));
      }
      else {
        if (staticSplits == null) {
          staticSplits = getSplits("/data/diagblock.input0", input0Len,
                                   split0Len, randFactor);
        }
        inputs.addAll(staticSplits);
      }
      int inputs0Size = inputs.size();
      inputs.addAll(getSplits("/data/diagblock.input1" + iteration, 
                              input1Len, split1Len, randFactor));
      int inputs1Size = inputs.size() - inputs0Size;
      for (int i = 0; i < numBlocks; ++i) {
        int remain = inputs0Size - (inputs0Size / numBlocks) * numBlocks;
        int fullsize = inputs0Size / numBlocks + ((i < remain) ? 1 : 0);
        for (int j = 0; j < fullsize; ++j) {
          indices.add("input0_" + i);
        }
      }
      for (int i = 0; i < numBlocks; ++i) {
        int remain = inputs1Size - (inputs1Size / numBlocks) * numBlocks;
        int fullsize = inputs1Size / numBlocks + ((i < remain) ? 1 : 0);
        for (int j = 0; j < fullsize; ++j) {
          indices.add("input1_" + i);
        }
      }
      job.formatTasks(indices, inputs);
    }
  }

  static class CirShflBlock extends DiagBlock {

    public CirShflBlock(Configuration conf) {
      super(conf);
    }

    public void configJob(int iteration) {
      long input0Len = conf.getLong("diagblock.input0.length", 1024 * 10);
      long input1Len = conf.getLong("diagblock.input1.length", 1024 * 10);
      long split0Len = conf.getLong("diagblock.split0.length", 128);
      long split1Len = conf.getLong("diagblock.split1.length", 128);
      int numBlocks = conf.getInt("diagblock.num.blocks", 16);
      float randFactor = conf.getFloat("job.split.length.rand", 0.1f);
      job.setInputs(
          Arrays.asList(
              new Segment(new Path("/data/cirshflblock.input0"), 0, input0Len),
              new Segment(new Path("/data/cirshflblock.input1" + iteration), 
                          0, input1Len)));
      List<Segment> inputs = new ArrayList<Segment>();
      List<String> indices = new ArrayList<String>();
      if ((!conf.getBoolean("job.has.static", true))) {
        inputs.addAll(getSplits("/data/cirshflblock.input0", input0Len,
                                split0Len, randFactor));
      }
      else {
        if (staticSplits == null) {
          staticSplits = getSplits("/data/cirshflblock.input0", input0Len,
                                   split0Len, randFactor);
        }
        inputs.addAll(staticSplits);
      }
      int input0Size = inputs.size();
      inputs.addAll(getSplits("/data/cirshflblock.input1" + iteration, 
                              input1Len, split1Len, randFactor));
      int input1Size = inputs.size() - input0Size;
      for (int i = 0; i < input0Size; ++i) {
        indices.add("input0_" + (i % numBlocks));
      }
      for (int i = 0; i < input1Size; ++i) {
        indices.add("input1_" + (i % numBlocks));
      }
      job.formatTasks(indices, inputs);
    }
  }

  static class RandShflBlock extends DiagBlock {

    protected List<String> staticIndices = null;

    public RandShflBlock(Configuration conf) {
      super(conf);
    }

    public void configJob(int iteration) {
      long input0Len = conf.getLong("diagblock.input0.length", 1024 * 10);
      long input1Len = conf.getLong("diagblock.input1.length", 1024 * 10);
      long split0Len = conf.getLong("diagblock.split0.length", 128);
      long split1Len = conf.getLong("diagblock.split1.length", 128);
      int numBlocks = conf.getInt("diagblock.num.blocks", 16);
      float randFactor = conf.getFloat("job.split.length.rand", 0.1f);
      job.setInputs(
          Arrays.asList(
              new Segment(new Path("/data/randshflblock.input0"), 0, input0Len),
              new Segment(new Path("/data/randshflblock.input1" + iteration), 
                          0, input1Len)));
      List<Segment> inputs = new ArrayList<Segment>();
      List<String> indices = new ArrayList<String>();
      if ((!conf.getBoolean("job.has.static", true))) {
        inputs.addAll(getSplits("/data/randshflblock.input0", input0Len,
                                split0Len, randFactor));
      }
      else {
        if (staticSplits == null) {
          staticSplits = getSplits("/data/randshflblock.input0", input0Len,
                                   split0Len, randFactor);
        }
        inputs.addAll(staticSplits);
      }
      int input0Size = inputs.size();
      inputs.addAll(getSplits("/data/randshflblock.input1" + iteration, 
                              input1Len, split1Len, randFactor));
      int input1Size = inputs.size() - input0Size;
      if ((!conf.getBoolean("job.has.static", true))) {
        for (int i = 0; i < input0Size; ++i) {
          indices.add("input0_" + (i % numBlocks));
        }
        Collections.shuffle(indices);
      }
      else {
        if (staticIndices == null) {
          staticIndices = new ArrayList<String>();
          for (int i = 0; i < input0Size; ++i) {
            staticIndices.add("input0_" + (i % numBlocks));
          }
          Collections.shuffle(staticIndices);
        }
        indices.addAll(staticIndices);
      }
      List<String> indices1 = new ArrayList<String>();
      for (int i = 0; i < input1Size; ++i) {
        indices1.add("input1_" + (i % numBlocks));
      }
      Collections.shuffle(indices1);
      indices.addAll(indices1);
      job.formatTasks(indices, inputs);
    }
  }

  static class RandPair extends SimJobClient {
    float denseLevel;
    Random r = new Random();
    protected List<Segment> staticSplits = null;

    public class RandPairFilter implements Map2Filter {
      public boolean accept(String s0, String s1) {
        if (!s0.contains("input0")) return false;
        if (!s1.contains("input1")) return false;
        if (r.nextFloat() < denseLevel) return true;
        return false;
      }
    }

    public RandPair(Configuration conf) {
      super(conf);
      job.setFilter(new RandPairFilter());
      denseLevel = conf.getFloat("randpair.dense.level", 0.2f);
    }

    public void configJob(int iteration) {
      long input0Len = conf.getLong("randpair.input0.length", 1024 * 10);
      long input1Len = conf.getLong("randpair.input1.length", 1024 * 10);
      long split0Len = conf.getLong("randpair.split0.length", 128);
      long split1Len = conf.getLong("randpair.split1.length", 128);
      float randFactor = conf.getFloat("job.split.length.rand", 0.1f);
      job.setInputs(
          Arrays.asList(
              new Segment(new Path("/data/randpair.input0"), 0, input0Len),
              new Segment(new Path("/data/randpair.input1" + iteration), 
                          0, input1Len)));
      List<Segment> inputs = new ArrayList<Segment>();
      if ((!conf.getBoolean("job.has.static", true))) {
        inputs.addAll(getSplits("/data/randpair.input0", input0Len,
                                split0Len, randFactor));
      }
      else {
        if (staticSplits == null) {
          staticSplits = getSplits("/data/randpair.input0", input0Len,
                                   split0Len, randFactor);
        }
        inputs.addAll(staticSplits);
      }
      inputs.addAll(getSplits("/data/randpair.input1" + iteration, 
                              input1Len, split1Len, randFactor));
      job.formatTasks(inputs);
    }
  }

  public static void main(String[] args) { 
    Configuration conf = new Configuration();
    conf.addResource("sim-sched-conf.xml");
    try {
      Class JobClientClass = conf.getClass("job.class.name", 
                                           Class.forName("TestScheduler$AllPair"));
      System.out.println("Job " + JobClientClass.toString());
      Class[] paramType = new Class[1];
      paramType[0] = Configuration.class;
      Constructor ctor = JobClientClass.getDeclaredConstructor(paramType);
      SimJobClient client = (SimJobClient) ctor.newInstance(conf);
      client.run();
    }
    catch (Exception e) {
      System.out.println("Exception: " + StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

}
