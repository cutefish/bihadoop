import java.util.Arrays;
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

  private Set<Integer> freeNodes = new HashSet<Integer>();

  public SimDistributedSystem(Configuration conf) {
    this.conf = conf;
    this.schedChoice = conf.getInt("node.schedule.choice", 0);
    this.numNodes = conf.getInt("num.distributed.node", 16);
    this.numReplicas = conf.getInt("num.replicas", 3);
    this.blockLen = conf.getInt("node.block.len", 64);
    this.nodes = new SimNode[numNodes];
    for (int i = 0; i < numNodes; ++i) {
      SimNode n = new SimNode(i);
      nodes[i] = n;
    }
    this.replicaInfo = new HashMap<Segment, LinkedList<Integer>>();
  }

  private void addFreeNodes() {
    for (int i = 0; i < numNodes; ++i) {
      freeNodes.add(i);
    }
  }

  private int pickRandomFreeNode() {
    Random r = new Random();
    if (freeNodes.isEmpty()) {
      addFreeNodes();
    }
    int i = r.nextInt(freeNodes.size());
    int count = 0;
    int ret = 0;
    for (int node : freeNodes) {
      if (i == count) {
        ret = node;
        break;
      }
      count ++;
    }
    freeNodes.remove(ret);
    return ret;
  }

  private List<Segment> getSegmentBlocks(Segment s) {
    List<Segment> ret = new ArrayList<Segment>();
    long start = (s.getOffset() / blockLen) * blockLen;
    long end = s.getOffset() + s.getLength();
    for (long i = start; i < end; i += blockLen) {
      ret.add(new Segment(s.getPath(), i, blockLen));
    }
    return ret;
  }

  public class SimNode {
    public final int nodeId;
    public final long diskCapacity;
    public final long memoryCapacity;

    private Set<Segment> replicas;
    private LinkedList<Segment> cache;
    private long cachedSize = 0;
    private int hits;
    private int misses;

    public SimNode(int id) {
      nodeId = id;
      this.diskCapacity = conf.getLong("node.disk.capacity", 1024 * 10);
      this.memoryCapacity = conf.getLong("node.memory.capacity", 256);
      replicas = new HashSet<Segment>();
      cache = new LinkedList<Segment>();
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
        if (replicaInfo.get(b) == null) {
          throw new RuntimeException("Segment does not exist: " + b);
        }
        if (replicas.contains(b)) {
          hits ++;
          LOG.debug("node#" + nodeId +
                    " read " + b.toString() + " : hit");
        }
        else {
          misses ++;
          LOG.debug("node#" + nodeId + 
                    " read " + b.toString() + " : miss");
        }
      }
    }

    //access a segment, cache block if not replicated
    public void cachedRead(Segment s) {
      List<Segment> blocks = getSegmentBlocks(s);
      for (Segment b : blocks) {
        if (replicaInfo.get(b) == null) {
          throw new RuntimeException("Segment does not exist: " + b);
        }
        if ((replicas.contains(b)) || (hasCached(b))) {
          hits ++;
          //renew the footprint
          cache.remove(b);
          cache.addLast(b);
          LOG.debug("node#" + nodeId +
                    " read " + b.toString() + " : hit");
          continue;
        }
        if (cachedSize >= diskCapacity) {
          //remove the first that is not a replica
          Segment toremove = null;
          for (int i = 0; i < cache.size(); ++i) {
            if (!replicas.contains(cache.get(i))) {
              toremove = cache.get(i);
              break;
            }
          }
          if ((toremove == null) && (diskCapacity != 0)) {
            throw new RuntimeException("no segment cached, but cache full");
          }
          if (toremove != null) {
            LOG.debug("disk size exceeds. " +
                      " curr: " + cachedSize + 
                      " cap: " + diskCapacity + 
                      " toremove: " + toremove);
            cache.remove(toremove);
            cachedSize -= toremove.getLength();
          }
        }
        cache.addLast(b);
        cachedSize += b.getLength();
        misses ++;
        LOG.debug("node#" + nodeId + 
                  " read " + b.toString() + " : miss");
      }
    }

    public boolean hasCached(Segment b) {
      int idx = cache.indexOf(b);
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
      ret.append("node#" + nodeId + ". ");
      ret.append("d: " + diskCapacity + ", " + 
                 "m: " + memoryCapacity + ", ");
      ret.append("r[" + replicas.size() + ": ");
      for (Segment seg : replicas) {
        ret.append(seg.toString() + ", ");
      }
      ret.append("] ");
      ret.append("    cr[" + cache.size() + ": ");
      for (Segment seg : cache) {
        ret.append(seg.toString() + ", ");
      }
      ret.append("] ");
      int count = 0;
      for (Segment seg : cache) {
        if (!replicas.contains(seg)) {
          count ++;
        }
      }
      ret.append("    c[" + count + ": ");
      for (Segment seg : cache) {
        if (!replicas.contains(seg)) {
          ret.append(seg.toString() + ", ");
        }
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

  public void createReplicas(Collection<Segment> segments) {
    for (Segment seg : segments) {
      List<Segment> blocks = getSegmentBlocks(seg);
      for (Segment b : blocks) {
        if (replicaInfo.get(b) != null) continue;
        Random r = new Random();
        LinkedList<Integer> locations = new LinkedList<Integer>();
        //do not put replica on the same node twice
        Set<SimNode> left = new HashSet<SimNode>(Arrays.asList(nodes));
        for (int i = 0; i < numReplicas; ++i) {
          int randCount = r.nextInt(left.size());
          int count = 0;
          SimNode picked = null;
          for (SimNode n : left) {
            if (count == randCount) {
              picked = n;
              break;
            }
            count ++;
          }
          left.remove(picked);
          int nodeId = picked.nodeId;
          locations.add(nodeId);
          nodes[nodeId].addReplica(b);
        }
        replicaInfo.put(b, locations);
      }
    }
  }

  public SimNode getNode(int nodeId) {
    return nodes[nodeId];
  }

  public String getHitRate() {
    int totalHit = 0;
    int totalMiss = 0;
    for (SimNode node : nodes) {
      totalHit += node.hits;
      totalMiss += node.misses;
    }
    return new String(">>hits: " + totalHit + 
                      " >>misses: " + totalMiss + 
                      " >>rate: "  + 
                      (float)totalHit / (float)(totalHit + totalMiss));
  }

  private void runJobMap2(SimJob job) {
    int finishedMaps = 0;

    List<Segment[]> tasks = job.getTasks();
    Map<Integer, Pack> memPacks = new HashMap<Integer, Pack>();
    Map<Integer, Pack> diskPacks = new HashMap<Integer, Pack>();

    MapTaskPacker packer = new MapTaskPacker(conf);
    Map<Segment, Segment> coverMap = buildCoverMap(tasks);
    packer.init(tasks, coverMap, -1, -1);

    Map<SegmentPair, Integer> scheduledTasks = 
        new HashMap<SegmentPair, Integer>();

    //this is used for default hadoop scheduling
    Map<Integer, LinkedList<Segment[]>> taskCache =
        new HashMap<Integer, LinkedList<Segment[]>>();

    for (Segment[] pair : tasks) {
      List<Segment> segments = getSegmentBlocks(pair[0]);
      segments.addAll(getSegmentBlocks(pair[1]));
      Set<Integer> locations = new HashSet<Integer>();
      for (Segment seg : segments) {
        List<Integer> locs = replicaInfo.get(seg);
        locations.addAll(locs);
      }
      for (int i : locations) {
        LinkedList<Segment[]> nodeTasks = taskCache.get(i);
        if (nodeTasks == null) {
          nodeTasks = new LinkedList<Segment[]>();
          taskCache.put(i, nodeTasks);
        }
        nodeTasks.add(pair);
      }
    }

    LOG.info("finished initialization");

    while (finishedMaps < tasks.size()) {
      //a wild node appears
      int nodeIndex = pickRandomFreeNode();
      SimNode node = getNode(nodeIndex);
      LOG.debug("node #" + nodeIndex + " asking for task.");
      LOG.debug("node info: " + node.toString());

      //get a task
      Segment[] task = null;
      while (task == null) {
        Pack memoryPack = memPacks.get(nodeIndex);
        if (memoryPack == null) {
          LOG.debug("Node: " + nodeIndex + " no memory pack available\n");
          Pack diskPack = diskPacks.get(nodeIndex);
          if (diskPack == null) {
            LOG.debug("Node: " + nodeIndex + " no disk pack available\n");
            try {
              diskPack = packer.obtainLastLevelPack(
                  new Segments(node.getReplicas()),
                  new Segments(node.getCache()),
                  node.diskCapacity,
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
              LOG.debug("Node: " + nodeIndex + 
                       " pack empty, possibly capacity not enough\n");
              break;
            }
            diskPacks.put(nodeIndex, diskPack);
            LOG.info("Node: " + nodeIndex + '\n' + 
                     "Level: " + "disk" + '\n' +
                     "Pack: " + '\n' + diskPack.toString() + '\n');
          }
          try {
            memoryPack = packer.obtainSubpack(diskPack, 
                                              node.memoryCapacity);
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
        if (!scheduledTasks.containsKey(info)) {
          scheduledTasks.put(info, nodeIndex);

          LOG.debug("Node: " + nodeIndex + " working on: \n" + 
                   "seg0: " + task[0].toString() + '\n' + 
                   "seg1: " + task[1].toString() + '\n');
          node.cachedRead(task[0]);
          node.cachedRead(task[1]);
          finishedMaps ++;
        }
      }
      else {
        //task is still null, disk pack empty, we resort to the default
        //locality1 scheduler.
        LOG.debug("using default locality1 scheduler");
        LinkedList<Segment[]> nodeTasks = taskCache.get(nodeIndex);
        if ((nodeTasks == null) && (!taskCache.isEmpty())) {
          Random r = new Random();
          int idx = r.nextInt(taskCache.size());
          int count = 0;
          for (int i : taskCache.keySet()) {
            if (idx == count) {
              nodeTasks = taskCache.get(i);
              break;
            }
            count ++;
          }
        }
        if (!nodeTasks.isEmpty()) {
          task = nodeTasks.pop();
        }
        if (task != null) {
          SegmentPair info = new SegmentPair(task[0], task[1]);
          if (!scheduledTasks.containsKey(info)) {
            scheduledTasks.put(info, nodeIndex);
            LOG.debug("Node: " + nodeIndex + " working on: \n" + 
                     "seg0: " + task[0].toString() + '\n' + 
                     "seg1: " + task[1].toString() + '\n');
            node.cachedRead(task[0]);
            node.cachedRead(task[1]);
            finishedMaps ++;
          }
        }
        if ((nodeTasks != null) && (nodeTasks.isEmpty())) {
          taskCache.remove(nodeIndex);
        }
      }
    }
    if ((!packer.isFinished())) {
      for (List<Segment[]> leftTasks : taskCache.values()) {
        for (Segment[] task : leftTasks) {
          SegmentPair info = new SegmentPair(task[0], task[1]);
          if (!scheduledTasks.containsKey(info)) {
            LOG.info("scheduledTasks size: " + scheduledTasks.size());
            LOG.info("finishedMaps size: " + finishedMaps);
            LOG.info("tasks size: " + tasks.size());
            LOG.info("JoinTable:\n" + packer.joinTableToString());
            LOG.info("Groups:\n" + packer.groupsToString());
            LOG.info("task cache:\n" + taskCache.toString());
            System.exit(-1);
          }
        }
      }
    }

    LOG.info("All tasks finsihed\n");
    LOG.info(getHitRate());
  }

  private Map<Segment, Segment> buildCoverMap(List<Segment[]> segments) {
    HashMap<Segment, Segment> ret = new HashMap<Segment, Segment>();
    for (Segment[] segs : segments) {
      for (Segment s : segs) {
        if (ret.get(s) != null) continue;
        long off = (s.getOffset() / blockLen) * blockLen;
        ret.put(s, new Segment(s.getPath(), off, blockLen));
      }
    }
    return ret;
  }

  private void runJobRandom(SimJob job) {

    LinkedList<Segment[]> tasks = new LinkedList(job.getTasks());
    while(!tasks.isEmpty()) {
      int nodeIndex = pickRandomFreeNode();
      SimNode node = getNode(nodeIndex);
      Segment[] task = tasks.pop();
      node.read(task[0]);
      node.read(task[1]);
    }
    LOG.info("All tasks finsihed\n");
    LOG.info(getHitRate());
  }

  private void runJobLocality1(SimJob job) {
    List<Segment[]> tasks = job.getTasks();
    LOG.info("total number of map tasks: " + tasks.size());
    Map<Integer, LinkedList<Segment[]>> taskCache =
        new HashMap<Integer, LinkedList<Segment[]>>();

    for (Segment[] pair : tasks) {
      List<Segment> segments = getSegmentBlocks(pair[0]);
      segments.addAll(getSegmentBlocks(pair[1]));
      Set<Integer> locations = new HashSet<Integer>();
      for (Segment seg : segments) {
        List<Integer> locs = replicaInfo.get(seg);
        locations.addAll(locs);
      }
      for (int i : locations) {
        LinkedList<Segment[]> nodeTasks = taskCache.get(i);
        if (nodeTasks == null) {
          nodeTasks = new LinkedList<Segment[]>();
          taskCache.put(i, nodeTasks);
        }
        nodeTasks.add(pair);
      }
    }

    Map<SegmentPair, Integer> scheduledTasks = 
        new HashMap<SegmentPair, Integer>();


    int finishedMaps = 0;

    while (finishedMaps < tasks.size()) {
      int nodeIndex = pickRandomFreeNode();
      SimNode node = getNode(nodeIndex);
      LOG.debug("node #" + nodeIndex + " asking for task.");
      LOG.debug("node info: " + node.toString());
      LinkedList<Segment[]> nodeTasks = taskCache.get(nodeIndex);
      Segment[] task = null;
      if ((nodeTasks == null) && (!taskCache.isEmpty())) {
        Random r = new Random();
        int idx = r.nextInt(taskCache.size());
        int count = 0;
        for (int i : taskCache.keySet()) {
          if (idx == count) {
            nodeTasks = taskCache.get(i);
            LOG.debug("steal task from node#" + i);
            break;
          }
          count ++;
        }
      }
      else {
        LOG.debug("get task from self queue");
      }
      while(!nodeTasks.isEmpty()) {
        task = nodeTasks.pop();
        SegmentPair info = new SegmentPair(task[0], task[1]);
        if (!scheduledTasks.containsKey(info)) {
          scheduledTasks.put(info, nodeIndex);
          finishedMaps ++;
          LOG.debug("node#" + nodeIndex + 
                    " seg0: " + task[0].toString() + 
                    " seg1: " + task[1].toString());
          node.read(task[0]);
          node.read(task[1]);
          break;
        }
      }
      LOG.debug("node#" + nodeIndex + 
                " task queue emptied or task executed");
    }

    LOG.info("All tasks finsihed\n");
    LOG.info(getHitRate());
  }

  public void runJob(SimJob job) {

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
