import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Segments;
import org.apache.hadoop.map2.MapTaskPacker;
import org.apache.hadoop.map2.MapTaskPacker.Pack;

/* SimDistributedSystem.java
 */

public class SimDistributedSystem {

  private static final Log LOG = LogFactory.getLog(SimDistributedSystem.class);

  private final Configuration conf;
  private final int schedChoice;
  private final int numNodes;
  private final int numReplicas;
  private final long blockLen;

  private SimNode[] nodes;
  private Map<Segment, LinkedList<Integer>> replicaInfo;

  public SimDistributedSystem() {
    this.conf = new Configuration();
    conf.addResource("sim-sched-conf.xml");
    init();
  }

  private List<Segment> getSegmentBlocks(Segment s) {
    List<Segment> ret = new ArrayList<Segment>();
    long off = (s.getOffset() / blockLen) * blockLen;
    long len = (s.getLength() / blockLen) * blockLen;
    for (long i = 0; i < len; i += blockLen) {
      ret.add(new Segment(s.getPath(), i, blockLen));
    }
    return ret;
  }

  public class SimNode {
    public final int nodeId;
    public final long diskCapacity;
    public final long memoryCapacity;

    private Set<Segment> replicas
    private List<Segment> cache;
    private int hits;
    private int misses;

    public SimNode(int id) {
      nodeId = id;
      this.diskCapacity = conf.getLong("node.disk.capacity", 1024 * 1024 * 1024 * 10);
      this.memoryCapacity = conf.getLong("node.memory.capacity", 1024 * 1024 * 256);
      replicas = new HashSet<Segment>();
      cache = new LinkedList<Segment>();
    }

    public String toString() {
      return "node#" + nodeId;
    }

    //replicas should be conformed to block configuration.
    public void addReplica(Segment s) {
      long off = (s.getOffset() / blockLen) * blockLen;
      replicas.add(new Segment(s.getPath(), off, blockLen));
    }

    //read a segment without cache
    public void read(Segment s) {
      List<Segment> blocks = getSegmentBlocks(s);
      for (Segment b : blocks) {
        if (replicas.contains(b)) {
          hits ++;
        }
        else {
          misses ++;
        }
      }
    }

    //access a segment, cache block if not replicated
    public void cachedRead(Segment s) {
      List<Segment> blocks = getSegmentBlocks(s);
      for (Segment b : blocks) {
        if ((replicas.contains(b)) || (hasCached(b))) {
          hits ++;
          continue;
        }
        cache.pollFirst();
        cache.addLast(b);
        misses ++;
      }
    }

    public boolean hasCached(Segment b) {
      int idx = Collections.binarySearch(cache, b);
      if (idx < 0) return false;
      return true;
    }


    public Collection<Segment> getReplicas() {
      return replicas;
    }

    public Collection<Segment> getCache() {
      return cache;
    }

    public String toString() {
      StringBuilder ret = new StringBuilder();
      ret.append("d: " + diskCapacity + 
                 "m: " + memoryCapacity + " ");
      ret.append("r[" + replicas.size() + ": ");
      for (Segment seg : replicas) {
        ret.append(seg.toString() + ", ");
      }
      ret.append("] ");
      ret.append("c[" + cache.size() + ": ");
      for (Segment seg : cache) {
        ret.append(seg.toString() + ", ");
      }
      ret.append("] ");
      return ret.toString();
    }
  }

  public static class SegmentPair {

    Segment seg0;
    Segment seg1;
    
    public SegmentPair(Segment s0, Segment s1) {
      if (s0.compareTo(s1) < 0) {
        seg0 = s0;
        seg1 = s1;
      }
      else {
        seg1 = s0;
        seg0 = s1;
      }
    }

    static boolean isEqual(Object a, Object b) {
      return a == b || (a != null & a.equals(b));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj != null && obj instanceof SegmentPair) {
        SegmentPair that = (SegmentPair)obj;
        return isEqual(this.seg0, that.seg0)
            && (this.seg1 == that.seg1);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return seg0.hashCode() + seg1.hashCode();
    }
  }

  public void init() {
    schedChoice = conf.getInt("node.schedule.choice", 0);
    numNodes = conf.getInt("num.distributed.node", 100);
    numReplicas = conf.getInt("num.replicas", 3);
    blockLen = conf.getInt("node.block.len", 64 * 1024 * 1024);
    nodes = new SimNode[numOfNodes];
    for (int i = 0; i < numOfNodes; ++i) {
      SimNode n = new SimNode(i);
      nodes[i] = n;
    }
    replicaInfo = new HashMap<Segment, LinkedList<Integer>>();
  }

  private void createReplicas(Collection<Segment> segments) {
    for (Segment seg : segments) {
      List<Segment> blocks = getSegmentBlocks(seg);
      for (Segment b : blocks) {
        if (replicaInfo.get(s) != null) return;
        Random r = new Random();
        LinkedList<Integer> locations = new LinkedList<Integer>();
        for (int i = 0; i < numReplicas; ++i) {
          int nodeId = r.nextInt(numNodes);
          locations.add(nodeId);
          nodes[nodeId].addReplica(b);
        }
        replicaInfo.put(s, locations);
      }
    }
  }

  public SimNode getNode(int nodeId) {
    return nodes[nodeId];
  }

  public float getHitRate() {
    int totalHit = 0;
    int totalMiss = 0;
    for (SimNode node : nodes) {
      totalHit += node.hits;
      totalMiss += node.misses;
    }
    return (float)totalHit / (float)totalMiss;
  }

  private void runJobMap2(List<Segment[]> tasks) {
    int finishedMaps = 0;
    int hit = 0;
    int miss = 0;

    Map<Integer, Pack> memoryPacks = new HashMap<Integer, Pack>();
    Map<Integer, Pack> diskPacks = new HashMap<Integer, Pack>();

    MapTaskPacker packer = new MapTaskPacker(conf);
    Map<Segment, Segment> coverMap = buildCoverMap(tasks);
    packer.init(tasks, coverMap, -1, -1);

    Map<SegmentPair, Integer> scheduledTasks = 
        new HashMap<SegmentPair, Integer>();

    LOG.info("finished initialization");

    while (finishedMaps < tasks.size()) {
      //a wild node appears
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      SimNode node = getNode(nodeIndex);
      LOG.info("node #" + nodeIndex + " asking for task." + 
               " info: " + node.toString());

      //get a task
      Segment[] task = null;
      while (task == null) {
        Pack memoryPack = memPacks.get(nodeIndex);
        if (memoryPack == null) {
          LOG.info("Node: " + nodeIndex + " no memory pack available\n");
          Pack diskPack = diskPacks.get(nodeIndex);
          if (diskPack == null) {
            LOG.debug("Node: " + nodeIndex + " no disk pack available\n");
            try {
              diskPack = packer.obtainLastLevelPack(
                  new Segments(node.getStaticSplits()),
                  new Segments(node.getDynamicSplits()),
                  node.getDiskCapacity(),
                  nodes.length);
            }
            catch (Exception e) {
              LOG.error("Exception: " + StringUtils.stringifyException(e));
              System.exit(-1);
            }
            if (diskPack == null) {
              LOG.debug("Node: " + nodeIndex + 
                       " cannot get a disk level pack\n");
              break;
            }
            if (diskPack.isEmpty()) {
              LOG.info("Node: " + nodeIndex + 
                       " pack empty, possibly capacity not enough\n");
              break;
            }
            diskPacks.put(nodeIndex, diskPack);
            LOG.debug("Node: " + nodeIndex + '\n' + 
                     "Level: " + "disk" + '\n' +
                     "Pack: " + '\n' + diskPack.toString() + '\n');
          }
          try {
            memoryPack = packer.obtainSubpack(diskPack, 
                                              node.getMemoryCapacity());
          }
          catch (Exception e) {
            LOG.error("Exception: " + StringUtils.stringifyException(e));
            System.exit(-1);
          }
          if (memoryPack == null) {
            LOG.debug("Node: " + nodeIndex + " finished a disk pack\n");
            diskPacks.remove(nodeIndex);
            continue;
          }
          LOG.debug("Node: " + nodeIndex + '\n' + 
                   "Level: " + "memory" + '\n' +
                   "Pack: " + '\n' + memoryPack.toString() + '\n');
          memPacks.put(nodeIndex, memoryPack);
        }
        task = memoryPack.getNext();
        if (task == null) {
          LOG.debug("Node: " + nodeIndex + " finished a memory pack\n");
          memPacks.remove(nodeIndex);
        }
      }

      if (task != null) {
        SegmentPair info = new SegmentPair(task[0], task[1]);
        if (scheduledTasks.containsKey(info)) {
          LOG.error("Already exist\ntask: " + info.toString() +
                   "\nnode: " + scheduledTasks.get(info));
          System.exit(-1);
        }
        scheduledTasks.put(info, nodeIndex);

        LOG.debug("Node: " + nodeIndex + " working on: \n" + 
                  "split: " + task[0].toString() + '\n' + 
                  "split: " + task[1].toString() + '\n');
        if (node.hasInStatic(task[0]) || node.hasInDynamic(task[0]))
          hit ++;
        else
          miss ++;
        if (node.hasInStatic(task[1]) || node.hasInDynamic(task[1]))
          hit ++;
        else
          miss ++;
        cacheAt(task[0], nodeIndex);
        cacheAt(task[1], nodeIndex);
        finishedMaps ++;
      }
    }
    LOG.info("All tasks finsihed\n");
    LOG.info("Hit: " + hit + " Miss: " + miss + '\n');
    if (!packer.isFinished()) {
      LOG.info("JoinTable:\n" + packer.joinTableToString());
      LOG.info("Groups:\n" + packer.groupsToString());
    }

    if (!packer.isFinished()) {
      System.exit(-1);
    }
  }

  private Map<Segment, Segment> buildCoverMap(List<Segment[]> segments) {
    HashMap<Segment, Segment> ret = new HashMap<Segment, Segment>();
    for (Segment[] segs : segments) {
      for (Segment s : segs) {
        if (ret.contains(s)) continue;
        long off = (s.getOffset() / blockLen) * blockLen;
        ret.add(s, new Segment(s.getPath(), off, blockLen));
      }
    }
    return ret;
  }

  private void runJobRandom(FakeJob job) {
    LinkedList<Segment[]> taskList = new LinkedList<Segment[]>(
        job.getTaskList());

    int hit = 0;
    int miss = 0;

    while(!taskList.isEmpty()) {
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      FakeNode node = getNode(nodeIndex);
      Segment[] task = taskList.pop();
      if (node.hasInStatic(task[0]) || node.hasInDynamic(task[0]))
        hit ++;
      else
          miss ++;
      if (node.hasInStatic(task[1]) || node.hasInDynamic(task[1]))
        hit ++;
      else
        miss ++;
    }
    LOG.info("All tasks finsihed\n");
    LOG.info("Hit: " + hit + " Miss: " + miss + '\n');
  }

  private void runJobLocality1(FakeJob job) {
    Map<Integer, LinkedList<Segment[]>> taskCache =
        new HashMap<Integer, LinkedList<Segment[]>>();

    for (Segment[] pair : job.getTaskList()) {
      List<Integer> locations = locationCache.get(pair[0]);
      for (int i : locations) {
        LinkedList<Segment[]> tasks = taskCache.get(i);
        if (tasks == null) {
          tasks = new LinkedList<Segment[]>();
          taskCache.put(i, tasks);
        }
        tasks.add(pair);
      }
    }

    Map<SegmentPair, Integer> scheduledTasks = 
        new HashMap<SegmentPair, Integer>();


    int finishedMaps = 0;
    int hit = 0;
    int miss = 0;

    while (finishedMaps < job.getNumTasks()) {
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      FakeNode node = getNode(nodeIndex);
      LinkedList<Segment[]> tasks = taskCache.get(nodeIndex);
      Segment[] task = null;
      if (!tasks.isEmpty()) {
        task = tasks.pop();
      }
      if (task != null) {
        SegmentPair info = new SegmentPair(task[0], task[1]);
        if (!scheduledTasks.containsKey(info)) {
          scheduledTasks.put(info, nodeIndex);
          finishedMaps ++;
          if (node.hasInStatic(task[0]) || node.hasInDynamic(task[0]))
            hit ++;
          else
            miss ++;
          if (node.hasInStatic(task[1]) || node.hasInDynamic(task[1]))
            hit ++;
          else
          miss ++;
        }
      }
    }

    LOG.info("All tasks finsihed\n");
    LOG.info("Hit: " + hit + " Miss: " + miss + '\n');
  }

  public void runJob(FakeJob job) {

    LOG.info("start running job");
    switch (schedChoice) {
      case 0: runJobMap2(job);
              break;
      case 1: runJobLocality1(job);
              break;
      case 2: runJobRandom(job);
              break;
      default:LOG.info("Invalid choice");
              break;
    }
  }

}
