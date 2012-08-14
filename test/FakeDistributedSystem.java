import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.mp2.IndexedSplit;

/* FakeDistributedSystem.java
 */

public class FakeDistributedSystem {

  private static final Log LOG = LogFactory.getLog(BlockCacheServer.class);

  private Configuration conf;
  private int numOfNodes;
  private int numOfReplicas;
  private FakeNode[] nodes;
  private Map<IndexedSplit, Node[]> staticLocations;

  public FakeDistributedSystem(Configuration conf) {
    this.conf = conf;
  }

  public class FakeNode {
    private final int nodeId;

    private long diskCapacity;
    private long memoryCapacity;
    private Set<IndexedSplit> staticSplits;
    private Set<IndexedSplit> dynamicSplits 

    public FakeNode(int id) {
      nodeId = id;
      this.diskCapacity = conf.getLong("node.disk.capacity", 16000);
      this.memoryCapacity = conf.getLong("node.memory.capacity", 1700);
      int cacheSize = diskCapacity / conf.getInt("split.block.size", 64);
      staticSplits = new HashSet<IndexedSplit>;
      dynamicSplits = new LinkedHashMap<IndexedSplit, boolean>() {
          protected boolean removeEldestEntry(
              Map.Entry<IndexedSplit, boolean>) {
            return size() > cacheSize;
          }
        };
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

    public List<IndexedSplit> getStaticSplits() {
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
  }

  public void init() {
    numOfNodes = conf.getInt("num.of.distributed.node", 100);
    numOfReplicas = conf.getInt("num.of.replicas", 3);
    nodes = new FakeNode[numOfNodes];
    for (int i = 0; i < numOfNodes; ++i) {
      FakeNode n = this.new FakeNode(i);
      nodes[i] = n;
    }
  }

  private void createSplit(IndexedSplit s) {
    if (staticLocations.getKey(s) == null) return;
    Random r = new Random();
    for (int i = 0; i < numOfReplicas; ++i) {
      int nodeId = r.nextInt(numOfNodes);
      nodeId.addStaticSplit(s);
    }
  }

  private void cacheAt(IndexedSplit s, int nodeId) {
    nodes[nodeId].addDynamicSplit(s);
  }

  public FakeNode getNode(int nodeId) {
    return nodes[nodeId];
  }

  public void submitJob(FakeJob job) {
    job.init();
    for (IndexedSplit split : job.getSplit0()) {
      createSplit(split);
    }
    for (IndexedSplit split : job.getSplit1()) {
      createSplit(split);
    }
  }

  public void runJob(FakeJob job) {
    int finishedMaps = 0;
    Map<Integer, Pack> memoryPackCache = new HashMap<Integer, Pack>();
    Map<Integer, Pack> diskPackCache = new HashMap<Integer, Pack>();
    MapTaskPacker packer = new MapTaskPacker();
    packer.init(job.getTaskList(), nodes.size());
    while (finishedMaps < job.getNumTasks()) {
      //a wild node appears
      Random r = new Random();
      int nodeIndex = r.nextInt(nodes.size());
      FakeNode node = getNode(nodeIndex);

      //get a task
      IndexedSplit[] task = null;
      boolean noTaskForNode = false;
      while ((task == null) && (!noTaskForNode)) {
        Pack memoryPack = memoryPackCache.get(nodeIndex);
        if (memoryPack == null) {
          Pack diskPack = diskPackCache.get(nodeIndex);
          if (diskPack == null) {
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
          Pack memoryPack = packer.obtainSubpack(diskPack, 
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
        LOG.debug("Node: " + nodeIndex + " working on: \n" + 
                  "split: " + task[0] + '\n' + 
                  "split: " + task[1] + '\n');
        finishedMaps ++;
      }
    }
  }

}
