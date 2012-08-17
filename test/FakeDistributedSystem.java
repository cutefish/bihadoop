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

import org.apache.hadoop.mapred.map2.IndexedSplit;
import org.apache.hadoop.mapred.map2.Map2SplitMetaInfo;
import org.apache.hadoop.mapred.map2.MapTaskPacker;
import org.apache.hadoop.mapred.map2.MapTaskPacker.Pack;

/* FakeDistributedSystem.java
 */

public class FakeDistributedSystem {

  private static final Log LOG = LogFactory.getLog(FakeDistributedSystem.class);

  private Configuration conf;
  private int schedChoice;
  private int numOfNodes;
  private int numOfReplicas;
  private FakeNode[] nodes;
  private Map<IndexedSplit, LinkedList<Integer>> locationCache;

  public FakeDistributedSystem(Configuration conf) {
    this.conf = conf;
    init();
  }

  public class FakeNode {
    private final int nodeId;

    private long diskCapacity;
    private long memoryCapacity;
    private Set<IndexedSplit> staticSplits;
    private Set<IndexedSplit> dynamicSplits;

    public FakeNode(int id) {
      nodeId = id;
      this.diskCapacity = conf.getLong("node.disk.capacity", 16000);
      this.memoryCapacity = conf.getLong("node.memory.capacity", 1700);
      final long cacheSize = diskCapacity / conf.getInt("split.block.size", 64);
      staticSplits = new HashSet<IndexedSplit>();
      dynamicSplits = Collections.newSetFromMap(
          new LinkedHashMap<IndexedSplit, Boolean>() {
          protected boolean removeEldestEntry(
              Map.Entry<IndexedSplit, Boolean> eldest) {
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

    public Set<IndexedSplit> getStaticSplits() {
      return staticSplits;
    }

    public Set<IndexedSplit> getDynamicSplits() {
      return dynamicSplits;
    }

    public void addStaticSplit(IndexedSplit s) {
      staticSplits.add(s);
    }

    public void addDynamicSplit(IndexedSplit s) {
      dynamicSplits.add(s);
    }

    public boolean hasInStatic(IndexedSplit s) {
      return staticSplits.contains(s);
    }

    public boolean hasInDynamic(IndexedSplit s) {
      return dynamicSplits.contains(s);
    }

    public String staticLocalToString() {
      return localToString(staticSplits);
    }

    public String dynamicLocalToString() {
      return localToString(dynamicSplits);
    }

    private String localToString(Set<IndexedSplit> splitSet) {
      StringBuilder ret = new StringBuilder();
      ret.append("[" + splitSet.size() + ": ");
      for (IndexedSplit split : splitSet) {
        ret.append(split.getIndex() + ", ");
      }
      ret.append("]");
      return ret.toString();
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
    locationCache = new HashMap<IndexedSplit, LinkedList<Integer>>();
  }

  private void createSplit(IndexedSplit s) {
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

  private void cacheAt(IndexedSplit s, int nodeId) {
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
    for (IndexedSplit split : job.getSplits()) {
      createSplit(split);
    }
  }

  private void runJobMap2(FakeJob job) {
    int finishedMaps = 0;
    int hit = 0;
    int miss = 0;

    Map<Integer, Pack> memoryPackCache = new HashMap<Integer, Pack>();
    Map<Integer, Pack> diskPackCache = new HashMap<Integer, Pack>();
    MapTaskPacker packer = new MapTaskPacker();
    packer.init(job.getTaskList(), nodes.length);
    if (packer.numGroups() > 4) {
      LOG.info(packer.groupsToString());
    }

    Map<Map2SplitMetaInfo, Integer> scheduledTasks = 
        new HashMap<Map2SplitMetaInfo, Integer>();

    LOG.info("finished initialization");

    while (finishedMaps < job.getNumTasks()) {
      //a wild node appears
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      FakeNode node = getNode(nodeIndex);
      LOG.info("node #" + nodeIndex + " asking for task");
      LOG.info("Node local: \n" + 
               "static: " + node.staticLocalToString() + "\n" + 
               "dynamic: " + node.dynamicLocalToString() + "\n");

      //get a task
      IndexedSplit[] task = null;
      boolean noTaskForNode = false;
      while ((task == null) && (!noTaskForNode)) {
        Pack memoryPack = memoryPackCache.get(nodeIndex);
        if (memoryPack == null) {
          LOG.info("Node: " + nodeIndex + " no memory pack available\n");
          Pack diskPack = diskPackCache.get(nodeIndex);
          if (diskPack == null) {
            LOG.info("Node: " + nodeIndex + " no disk pack available\n");
            diskPack = packer.obtainLastLevelPack(node.getStaticSplits(),
                                                  node.getDynamicSplits(),
                                                  node.getDiskCapacity());
            if (diskPack == null) {
              LOG.info("Node: " + nodeIndex + 
                       " cannot get a disk level pack\n");
              noTaskForNode = true;
              continue;
            }
            diskPackCache.put(nodeIndex, diskPack);
            LOG.info("Node: " + nodeIndex + '\n' + 
                     "Level: " + "disk" + '\n' +
                     "Pack: " + '\n' + diskPack.toString() + '\n');
          }
          memoryPack = packer.obtainSubpack(diskPack, 
                                            node.getMemoryCapacity());
          if (memoryPack == null) {
            LOG.info("Node: " + nodeIndex + " finished a disk pack\n");
            diskPackCache.remove(nodeIndex);
            continue;
          }
          LOG.info("Node: " + nodeIndex + '\n' + 
                   "Level: " + "memory" + '\n' +
                   "Pack: " + '\n' + memoryPack.toString() + '\n');
          memoryPackCache.put(nodeIndex, memoryPack);
        }
        task = memoryPack.getNext();
        if (task == null) {
          LOG.info("Node: " + nodeIndex + " finished a memory pack\n");
          memoryPackCache.remove(nodeIndex);
        }
      }

      if (task != null) {
        Map2SplitMetaInfo info = new Map2SplitMetaInfo(task[0], task[1]);
        if (scheduledTasks.containsKey(info)) {
          LOG.info("Already exist\ntask: " + info.toString() +
                   "\nnode: " + scheduledTasks.get(info));
          System.exit(-1);
        }
        scheduledTasks.put(info, nodeIndex);

        LOG.info("Node: " + nodeIndex + " working on: \n" + 
                  "split: " + task[0].getIndex() + '\n' + 
                  "split: " + task[1].getIndex() + '\n');
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
    LinkedList<IndexedSplit[]> taskList = new LinkedList<IndexedSplit[]>(
        job.getTaskList());

    int hit = 0;
    int miss = 0;

    while(!taskList.isEmpty()) {
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      FakeNode node = getNode(nodeIndex);
      IndexedSplit[] task = taskList.pop();
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
    Map<Integer, LinkedList<IndexedSplit[]>> taskCache =
        new HashMap<Integer, LinkedList<IndexedSplit[]>>();

    for (IndexedSplit[] pair : job.getTaskList()) {
      List<Integer> locations = locationCache.get(pair[0]);
      for (int i : locations) {
        LinkedList<IndexedSplit[]> tasks = taskCache.get(i);
        if (tasks == null) {
          tasks = new LinkedList<IndexedSplit[]>();
          taskCache.put(i, tasks);
        }
        tasks.add(pair);
      }
    }

    Map<Map2SplitMetaInfo, Integer> scheduledTasks = 
        new HashMap<Map2SplitMetaInfo, Integer>();


    int finishedMaps = 0;
    int hit = 0;
    int miss = 0;

    while (finishedMaps < job.getNumTasks()) {
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.length);
      FakeNode node = getNode(nodeIndex);
      LinkedList<IndexedSplit[]> tasks = taskCache.get(nodeIndex);
      IndexedSplit[] task = null;
      if (!tasks.isEmpty()) {
        task = tasks.pop();
      }
      if (task != null) {
        Map2SplitMetaInfo info = new Map2SplitMetaInfo(task[0], task[1]);
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
