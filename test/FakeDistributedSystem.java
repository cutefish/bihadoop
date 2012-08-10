import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.mp2.IndexedSplit;

/* FakeDistributedSystem.java
 */

public class FakeDistributedSystem {
  private Configuration conf;

  private int numOfNodes;
  private int numOfReplicas;
  private FakeNode[] nodes;
  private Map<IndexedSplit, Node[]> staticLocations;

  public class FakeNode {
    private final int nodeId;

    private List<IndexedSplit> staticSplits;
    private Set<IndexedSplit> dynamicSplits 

    public FakeNode(int id) {
      nodeId = id;
      int cacheSize = conf.getInt("node.split.cache.size", 100);
      staticSplits = new List<IndexedSplit>;
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

  public void createSplit(IndexedSplit s) {
    if (staticLocations.getKey(s) == null) return;
    Random r = new Random();
    for (int i = 0; i < numOfReplicas; ++i) {
      int nodeId = r.nextInt(numOfNodes);
      nodeId.addStaticSplit(s);
    }
  }

  public void cacheAt(IndexedSplit s, int nodeId) {
    nodes[nodeId].addDynamicSplit(s);
  }

  public FakeNode getNode(int nodeId) {
    return nodes[nodeId];
  }

}
