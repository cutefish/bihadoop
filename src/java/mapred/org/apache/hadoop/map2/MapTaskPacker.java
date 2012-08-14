package org.apache.hadoop.mapred.map2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

/* MapTaskPacker.java
 * Handles packing of map tasks.
 */

public class MapTaskPacker {

  private static final Log LOG = LogFactory.getLog(BlockCacheServer.class);

  private static final float ACCEPT_OVERLAP_RATIO = 0.8;
  private int totalNumMaps = 0;
  private int clusterSize = 0;
  private int maxPackSize = 0;
  private List<HashSet<IndexedSplit>> groupList;
  private Map<IndexedSplit, Integer> groupCache;
  private Map<IndexedSplit, TreeSet<IndexedSplit>> joinTable;

  /**
   * Initialize MapTaskPacker from a list of IndexedSplit[2].
   *
   * Splits are not ordered, that is, we do not distinguish 
   * split[0] and split[1]
   *
   * @param splitList List of joined splits.
   */
  public void init(List<IndexedSplit[]> splitList, int clusterSize) {
    this.clusterSize = clusterSize;
    initJoinTable(splitList);
    initGroups();
    maxPackSize = totalNumMaps / clusterSize + 
        ((totalNumMaps % clusterSize == 0) ? 0 : 1);
  }

  private void initJoinTable(List<IndexedSplit[]> splitList) {
    joinTable = new HashMap<IndexedSplit, TreeSet<IndexedSplit>>();

    //add each pair to two sets.
    for (splitPair : splitList) {
      for (int i = 0; i < 2; ++i) {
        TreeSet<IndexedSplit> set = joinTable.get(splitPair[i]);
        if (set == null) {
          set = new TreeSet<IndexedSplit>();
          ret.put(splitPair[i], set);
        }
        else {
          set.add(splitPair[1 - i]);
        }
        totalNumMaps ++;
      }
    }
  }

  private void initGroups() {
    groupList = new LinkedList<HashSet<IndexedSplit>>();
    groupCache = new HashMap<IndexedSplit, Integer>();

    //keep a cache for the largest set in a group
    largestCache = new LinkedList<TreeSet<IndexedSplit>>();

    //foreach in the joinTable
    for (Iterator<Map.Entry<IndexedSplit, TreeSet<IndexedSplit>>> 
        I = joinTable.entrySet(); I.hasNext();) {
      Map.Entry<IndexedSplit, TreeSet<IndexedSplit>> entry = I.next();
      IndexedSplit split = entry.getKey();
      TreeSet<IndexedSplit> splitJoinSet = entry.getValue();

      //foreach in the group list
      for (int i = 0; i < groupList.size(); ++i) {
        TreeSet<IndexedSplit> largest = largestCache.get(i);
        assert (largest != null) : ("Largest entry not constructed");
        if (shouldContain(largest, splitJoinSet)) {
          groupList.get(i).add(split);
          groupCache.put(split, i);
          //update largest cache
          if (splitJoinSet.size() > largest.size()) {
            largestCache.get(i) = splitJoinSet;
          }
          continue;
        }
      }

      //no group should contain this entry
      HashSet<IndexedSplit> newGroup = new HashSet<IndexedSplit>();
      newGroup.add(split);
      groupList.add(newGroup);
      largestCache.add(splitJoinSet);
    } 
  }

  private boolean shouldContain(Set<IndexedSplit> first,
                                Set<IndexedSplit> second) {
    Set<IndexedSplit> smaller;
    Set<IndexedSplit> larger;
    if (first.size() < second.size()) {
      smaller = first;
      larger = second;
    }
    else {
      larger = second;
      smaller = first;
    }
    int numOverlap = 0;
    for(IndexedSplit split : smaller) {
      if (larger.contains(split)) {
        numOverlap ++;
      }
    }
    return numOverlap > larger.size() * ACCEPT_OVERLAP_RATIO;
  }

  //for debug
  public void writeGroupList(DataOutput out) throws IOException {
    out.writeChars("groupNo " + groupList.size() + '\n');
    for (int i = 0; i < groupList.size(); ++i) {
      out.writeChars("group " + i + '\n');
      HashMap<IndexedList> group = groupList.get(i);
      for (IndexedList split0 : group) {
        out.writeChars("+split0: " + split0 + '\n');
        for (IndexedList split1 : joinTable.get(split0)) {
          out.writeChars("++split1: " + split1 + '\n');
        }
      }
    }
  }

  /**
   * Obtain last level pack
   *
   * For disk level cache, obtained directly from groups.
   *
   * @param staticCache static cache of splits on the node.
   * @param dynamicCache dynamic cache of splits on the node.
   * @param cacheSize dynamic cache size.
   * @return last level pack
   *
   * Synchronization simplifies the design of algorithm with some performance
   * hurt. The assumptions are that this function should be called not very
   * frequently and the cache size are not so large.
   */
  private synchronized Map<IndexedSplit, TreeSet<IndexedSplit>>
      obtainLastLevelPack(Set<IndexedSplit> staticCache, Set<IndexedSplit>
                          dynamicCache, long cacheSize) {

    //no pack if no group.
    if ((groupList == null) || (groupList.isEmpty())) return null;

    //pack from the group with most local splits
    //select tasks from the largest join set.
    HashSet<IndexedSplit> cache = new HashSet<IndexedSplit>();
    cahce.addAll(staticCache);
    cache.addAll(dynamicCache);
    int bestGroupIndex = chooseBestGroup(Set<IndexedSplit> cache);
    IndexedSplit chosenSplit = chooseLargestSet(bestGroupIndex, cache);

    //start packing
    int numPackedTasks = 0;
    long sizeLeft;
    boolean finished = false;
    Map<IndexedSplit, TreeSet<IndexedSplit>> pack = 
        new HashMap<IndexedSplit, TreeSet<IndexedSplit>>();
    Set<IndexedSplit> newDynamicCache = new HashSet<IndexedSplit>();
    // use half of the cache for the join set.
    sizeLeft = cacheSize / 2;
    TreeSet<IndexedSplit> tmp = joinTable.get(chosenSplit);
    TreeSet<IndexedSplit> chosenSet = new TreeSet<IndexedSplit>;
    for(IndexedSplit split : tmp) {
      newDynamicCache.add(split);
      chosenSet.add(split);
      sizeLeft -= split.size();
      if(sizeLeft < 0) break;
    }
    sizeLeft += cacheSize / 2;
    // obtain local tasks first
    for (IndexedSplit local : cache) {
      //only splits in dynamic cache will increase the size, others are local.
      if (dynamicCache.contains(local)) {
        if (sizeLeft - local.size() < 0) continue;
      }
      newDynamicCache.add(local);
      sizeLeft -= local.size();

      //grab all splits in chosenSet that are also in joinSet
      tmp = joinTable.get(local);
      for (IndexedSplit split : chosenSet) {
        if (tmp.contains(split)) {
          addPairToPack(packed, local, split);
          removePair(local, split);
          numPackedTasks ++;
          if (numPackedTasks >= maxPackSize) {
            finished = true;
            break;
          }
        }
      }
      if (finished == true) return pack;
    }

    // try all non-locals in the same group
    for (IndexedSplit any : groupList.get(bestGroupIndex)) {
      //check cache size
      if ((!staticCache.contains(any)) &&
          (!dynamicCache.contains(any))) {
        if (sizeLeft - local.size() < 0) {
          return pack;
        }
        else {
          newDynamicCache.add(any);
          sizeLeft -= any.size();
        }
      }
      
      //grab all splits in chosenSet that are also in joinSet
      tmp = joinTable.get(any);
      for (IndexedSplit split : chosenSet) {
        if (tmp.contains(split)) {
          addPairToPack(packed, any, split);
          removePair(any, split);
          numPackedTasks ++;
          if (numPackedTasks >= maxPackSize) {
            finished = true;
            break;
          }
        }
      }
      if (finished == true) return pack;
    }

    return pack;
  }

  /**
   * Obtain high level pack
   *
   * For memory level cache, obtained from a pack.
   */

  private int chooseBestGroup(Set<IndexedSplit> cache) {
    Map<Integer, Integer> groupForlocals = new HashMap<Integer, Integer>();
    for (IndexedSplit split : cache) {
      int groupIndex = groupCache.get(split);
      Integer frequency = groupForlocals.get(groupIndex);
      if (frequency == null) {
        groupForlocals.put(groupIndex, 1);
      }
      else {
        groupForlocals.put(groupIndex, frequency + 1);
      }
    }
    int bestGroupIndex = -1;
    int largest = 0;
    for (Map.Entry<Integer, Integer> entry : groupForlocals.entrySet()) {
      if (entry.getValue() > largest) {
        bestGroupIndex = entry.getKey();
        largest = entry.getValue();
      }
    }
    return (bestGroupIndex == -1) ? 0 : bestGroupIndex;
  }
  
  //choose the largest join set in a group
  private IndexedSplit chooseLargestSet(int groupIndex, 
                                        Set<IndexedSplit> cache) {
    int largest = 0;
    IndexedSplit chosenSplit = null;
    HashSet<IndexedSplit> group = groupList.get(groupIndex);
    for (IndexedSplit split : cache) {
      if ((group.contains(split)) && 
          (joinTable.get(split).size() > largest)) {
        largest = joinTable.get(split).size();
        chosenSplit = split;
      }
    }

    //this happens when no local is in a group
    //choose any split
    if (chosenSplit == null) {
      chosenSplit = group.iterator().next();
    }
    return chosenSplit;
  }

  private static void addPairToPack(
      Map<IndexedSplit, TreeSet<IndexedSplit>> pack,
      IndexedSplit split0, IndexedSplit split1) {
    TreeSet<IndexedSplit> set = pack.get(split0);
    if (set == null) {
      set = new TreeSet<IndexedSplit>();
      pack.put(split0, set);
    }
    else {
      set.add(split1);
    }
  }

  /**
   * Update members when move a pair of splits to pack This tries to maintain
   * the following rules.
   *
   * 1. a split is a key in joinTable if and only if the join set is non-empty;
   * 2. split has no order, (split0, split1), (split1, split0) * will both
   * appear or neither in joinTable; 
   * 3. a split is in groupList if and only if it is a key in joinTable; 
   * 4. a split is a key in groupCache if and only if it is in groupList.
   */
  private void removePair(IndexedSplit split0, IndexedSplit split1) {
    TreeSet<IndexedSplit> tmpSet;
    tmpSet = joinTable.get(split0);
    if (tmpSet == null) return;
    tmpSet.remove(split1);
    if (tmpSet.isEmpty()) {
      joinTable.remove(split0);
      groupList.get(groupCache.get(split0)).remove(split0);
      groupCache.remove(split0);
    }
    tmpSet = joinTable.get(split1);
    tmpSet.remove(split0);
    if (tmpSet.isEmpty()) {
      joinTable.remove(split0);
      groupList.get(groupCache.get(split0)).remove(split0);
      groupCache.remove(split0);
    }
  }

  /**
   * Convert Map to List and correct the order of the two splits.
   */
  public List<IndexedSplit[]> normalize(Map<IndexedList, TreeSet<IndexedSplit> map) {
    List<IndexedSplit[]> list = ArrayList<IndexedSplit[]>();
    for (IndexedList key : map.keySet()) {
      for (IndexedList value : map.getKey(key)) {
        IndexedSplit[] splitPair = new IndexedSplit[2];
        if (splitGroupOrder == 0) {
          splitPair[0] = key;
          splitPair[1] = value;
        }
        else {
          splitPair[1] = key;
          splitPair[0] = value;
        }
        list.add(splitPair);
      }
    }
  }

  /**
   * Pack to String.
   * For debug use.
   */
  public String packToString(List<IndexedSplit[]> pack) {
  }

}
