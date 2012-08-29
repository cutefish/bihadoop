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

/* FakeDistributedSystem.java
 */

public class FakeDistributedSystem {

  private static final Log LOG = LogFactory.getLog(FakeDistributedSystem.class);

  private Configuration conf;
  private int schedChoice;
  private int numOfNodes;
  private int numOfReplicas;
  private FakeNode[] nodes;
  private Map<Segment, LinkedList<Integer>> locationCache;

  public FakeDistributedSystem(Configuration conf) {
    this.conf = conf;
    init();
  }

  public class FakeNode {
    private final int nodeId;

    private long diskCapacity;
    private long memoryCapacity;
    private Set<Segment> staticSplits;
    private Set<Segment> dynamicSplits;

    public FakeNode(int id) {
      nodeId = id;
      this.diskCapacity = conf.getLong("node.disk.capacity", 16000);
      this.memoryCapacity = conf.getLong("node.memory.capacity", 1700);
      final long cacheSize = diskCapacity / conf.getInt("split.block.size", 64);
      staticSplits = new HashSet<Segment>();
      dynamicSplits = Collections.newSetFromMap(
          new LinkedHashMap<Segment, Boolean>() {
          protected boolean removeEldestEntry(
              Map.Entry<Segment, Boolean> eldest) {
          return size() > cacheSize;
          }
          });
    }

    public String toString() {
      return "node#" + nodeId;
    }

    public int getNodeId() {
      return nodeId;
    }

    public long getDiskCapacity() {
      return diskCapacity;
    }

    public long getMemoryCapacity() {
      return memoryCapacity;
    }

    public Set<Segment> getStaticSplits() {
      return staticSplits;
    }

    public Set<Segment> getDynamicSplits() {
      return dynamicSplits;
    }

    public void addStaticSplit(Segment s) {
      staticSplits.add(s);
    }

    public void addDynamicSplit(Segment s) {
      dynamicSplits.add(s);
    }

    public boolean hasInStatic(Segment s) {
      return staticSplits.contains(s);
    }

    public boolean hasInDynamic(Segment s) {
      return dynamicSplits.contains(s);
    }

    public String staticLocalToString() {
      return localToString(staticSplits);
    }

    public String dynamicLocalToString() {
      return localToString(dynamicSplits);
    }

    private String localToString(Set<Segment> segs) {
      StringBuilder ret = new StringBuilder();
      ret.append("[" + segs.size() + ": ");
      for (Segment seg : segs) {
        ret.append(seg.toString() + ", ");
      }
      ret.append("]");
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
    numOfNodes = conf.getInt("num.of.distributed.node", 100);
    numOfReplicas = conf.getInt("num.of.replicas", 3);
    nodes = new FakeNode[numOfNodes];
    for (int i = 0; i < numOfNodes; ++i) {
      FakeNode n = this.new FakeNode(i);
      nodes[i] = n;
    }
    locationCache = new HashMap<Segment, LinkedList<Integer>>();
  }

  private void createSplit(Segment s) {
    if (locationCache.get(s) != null) return;
    Random r = new Random();
    LinkedList<Integer> locationList = new LinkedList<Integer>();
    for (int i = 0; i < numOfReplicas; ++i) {
      int nodeId = r.nextInt(numOfNodes);
      locationList.add(nodeId);
      nodes[nodeId].addStaticSplit(s);
    }
    locationCache.put(s, locationList);
  }

  private void cacheAt(Segment s, int nodeId) {
    if(!nodes[nodeId].hasInStatic(s))
      nodes[nodeId].addDynamicSplit(s);
  }

  public FakeNode getNode(int nodeId) {
    return nodes[nodeId];
  }

  public void submitJob(FakeJob job) {
    LOG.info("submitted job");
    job.init();
    LOG.info("number of tasks: " + job.getNumTasks());
    for (Segment split : job.getSplits()) {
      createSplit(split);
    }
  }

  private void runJobMap2(FakeJob job) {
    int finishedMaps = 0;
    int hit = 0;
    int miss = 0;

    Map<Integer, Pack> memoryPackCache = new HashMap<Integer, Pack>();
    Map<Integer, Pack> diskPackCache = new HashMap<Integer, Pack>();
    MapTaskPacker packer = new MapTaskPacker(conf);
    packer.init(job.getTaskList());
    if (packer.numGroups() > 4) {
      LOG.info(packer.groupsToString());
    }

    Map<SegmentPair, Integer> scheduledTasks = 
        new HashMap<SegmentPair, Integer>();

    LOG.info("finished initialization");

    while (finishedMaps < job.getNumTasks()) {
      //a wild node appears
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      FakeNode node = getNode(nodeIndex);
      LOG.debug("node #" + nodeIndex + " asking for task");
      LOG.debug("Node local: \n" + 
               "static: " + node.staticLocalToString() + "\n" + 
               "dynamic: " + node.dynamicLocalToString() + "\n");

      //get a task
      Segment[] task = null;
      while (task == null) {
        Pack memoryPack = memoryPackCache.get(nodeIndex);
        if (memoryPack == null) {
          LOG.debug("Node: " + nodeIndex + " no memory pack available\n");
          Pack diskPack = diskPackCache.get(nodeIndex);
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
            diskPackCache.put(nodeIndex, diskPack);
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
            diskPackCache.remove(nodeIndex);
            continue;
          }
          LOG.debug("Node: " + nodeIndex + '\n' + 
                   "Level: " + "memory" + '\n' +
                   "Pack: " + '\n' + memoryPack.toString() + '\n');
          memoryPackCache.put(nodeIndex, memoryPack);
        }
        task = memoryPack.getNext();
        if (task == null) {
          LOG.debug("Node: " + nodeIndex + " finished a memory pack\n");
          memoryPackCache.remove(nodeIndex);
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
