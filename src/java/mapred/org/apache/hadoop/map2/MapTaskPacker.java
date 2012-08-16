package org.apache.hadoop.mapred.map2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/* MapTaskPacker.java
 * Handles packing of map tasks.
 */

public class MapTaskPacker {

  private static final Log LOG = LogFactory.getLog(MapTaskPacker.class);

  private static final float ACCEPT_OVERLAP_RATIO = 0.8f;
  private int totalNumMaps = 0;
  private int clusterSize = 0;
  private int maxPackSize = 0;
  private Map<Integer, HashSet<IndexedSplit>> groups;
  private Map<IndexedSplit, Integer> groupCache;
  private Map<IndexedSplit, TreeSet<IndexedSplit>> joinTable;

  /**
   * Pack of tasks(IndexedSplit pairs)
   */
  public static class Pack {
    private Map<IndexedSplit, TreeSet<IndexedSplit>> packMap;
    private IndexedSplit current = null;

    public Pack() {
      this.packMap = new HashMap<IndexedSplit, TreeSet<IndexedSplit>>();
    }

    public TreeSet<IndexedSplit> get(IndexedSplit split) {
      return packMap.get(split);
    }

    public void put(IndexedSplit split, TreeSet<IndexedSplit> set) {
      packMap.put(split, set);
    }

    public void remove(IndexedSplit split) {
      packMap.remove(split);
    }

    public boolean isEmpty() {
      return packMap.isEmpty();
    }

    public int size() {
      return packMap.size();
    }

    public Set<IndexedSplit> keySet() {
      return packMap.keySet();
    }

    public IndexedSplit[] getNext() {
      if ((packMap == null) || (packMap.isEmpty())) {
        return null;
      }
      if (current == null) {
        current = packMap.keySet().iterator().next();
      }

      TreeSet<IndexedSplit> set = packMap.get(current);
      while ((set == null) || (set.isEmpty())) {
        packMap.remove(current);
        if (packMap.isEmpty()) return null;
        current = packMap.keySet().iterator().next();
        set = packMap.get(current);
      }
      IndexedSplit joinSplit = set.first();
      set.remove(joinSplit);
      IndexedSplit[] nextSplits = new IndexedSplit[2];
      nextSplits[0] = current;
      nextSplits[1] = joinSplit;
      return nextSplits;
    }

    public String toString() {
      StringBuilder ret = new StringBuilder();
      for (Iterator<Map.Entry<IndexedSplit, TreeSet<IndexedSplit>>>
           I = packMap.entrySet().iterator(); I.hasNext();) {
        Map.Entry<IndexedSplit, TreeSet<IndexedSplit>> entry = I.next();
        IndexedSplit split0 = entry.getKey();
        ret.append(split0.getIndex() + " [");
        TreeSet<IndexedSplit> joinSet = entry.getValue();
        for (IndexedSplit split1 : joinSet) {
          ret.append(split1.getIndex() + ", ");
        }
        ret.append("]\n");
      }
      return ret.toString();
    }
  }

  /**
   * Initialize MapTaskPacker from a list of IndexedSplit[2].
   *
   * Splits are not ordered, that is, we do not distinguish 
   * split[0] and split[1]
   *
   * @param splitList List of joined splits.
   */
  public void init(List<IndexedSplit[]> splitList, int clusterSize) {
  //public void init(JobInProgress job, int clusterSize) {

    this.clusterSize = clusterSize;
    initJoinTable(splitList);
    initGroups();
    maxPackSize = totalNumMaps / clusterSize + 
        ((totalNumMaps % clusterSize == 0) ? 0 : 1);
//    LOG.debug("JoinTable:\n" + joinTableToString());
//    LOG.debug("Groups:\n" + groupsToString());
    LOG.info("JoinTable size: " + joinTable.size());
    LOG.info("Number of Groups size: " + groups.size());
    LOG.info("Group cache size: " + groupCache.size());
    LOG.info("Finished initializing for job");
  }

  private void initJoinTable(List<IndexedSplit[]> splitList) {
    joinTable = new HashMap<IndexedSplit, TreeSet<IndexedSplit>>();

    //add each pair to two sets.
    for (IndexedSplit[] splitPair : splitList) {
      for (int i = 0; i < 2; ++i) {
        TreeSet<IndexedSplit> set = joinTable.get(splitPair[i]);
        if (set == null) {
          set = new TreeSet<IndexedSplit>();
          joinTable.put(splitPair[i], set);
        }
        set.add(splitPair[1 - i]);
        totalNumMaps ++;
      }
    }
    
    totalNumMaps /= 2;
    LOG.info("Total Number of Tasks: " + totalNumMaps);
  }

  private void initGroups() {
    groups = new HashMap<Integer, HashSet<IndexedSplit>>();
    groupCache = new HashMap<IndexedSplit, Integer>();

    //keep a cache for the largest set in a group
    Map<Integer, TreeSet<IndexedSplit>> largestCache = 
        new HashMap<Integer, TreeSet<IndexedSplit>>();

    //foreach in the joinTable
    for (Iterator<Map.Entry<IndexedSplit, TreeSet<IndexedSplit>>> 
        I = joinTable.entrySet().iterator(); I.hasNext();) {
      Map.Entry<IndexedSplit, TreeSet<IndexedSplit>> entry = I.next();
      IndexedSplit split = entry.getKey();
      TreeSet<IndexedSplit> splitJoinSet = entry.getValue();

      boolean groupFound = false;
      //foreach in the group list
      for (int i = 0; i < groups.size(); ++i) {
        TreeSet<IndexedSplit> largest = largestCache.get(i);
        assert (largest != null) : ("Largest entry not constructed");
        if (shouldContain(largest, splitJoinSet)) {
          groups.get(i).add(split);
          groupCache.put(split, i);
          //update largest cache
          if (splitJoinSet.size() > largest.size()) {
            largestCache.put(i, splitJoinSet);
          }
          groupFound = true;
          break;
        }
      }

      if (groupFound) continue;

      //no group contains this entry
      HashSet<IndexedSplit> newGroup = new HashSet<IndexedSplit>();
      newGroup.add(split);
      groups.put(groups.size(), newGroup);
      largestCache.put(groups.size() - 1, splitJoinSet);
      groupCache.put(split, groups.size() - 1);
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
  public String joinTableToString() {
    StringBuilder ret = new StringBuilder();
    for (Iterator<Map.Entry<IndexedSplit, TreeSet<IndexedSplit>>>
         I = joinTable.entrySet().iterator(); I.hasNext();) {
      Map.Entry<IndexedSplit, TreeSet<IndexedSplit>> entry = I.next();
      IndexedSplit split0 = entry.getKey();
      ret.append(split0.getIndex() + " [");
      TreeSet<IndexedSplit> joinSet = entry.getValue();
      ret.append(joinSet.size() + ": ");
      for (IndexedSplit split1 : joinSet) {
        ret.append(split1.getIndex() + ", ");
      }
      ret.append("]\n");
    }
    return ret.toString();
  }

  //for debug
  public String groupsToString() {
    StringBuilder ret = new StringBuilder();
    ret.append("groupNo " + groups.size() + '\n');
    for (int i = 0; i < groups.size(); ++i) {
      ret.append("group " + i + '\n');
      HashSet<IndexedSplit> group = groups.get(i);
      for (IndexedSplit split0 : group) {
        ret.append(split0.getIndex() + " [");
        ret.append(joinTable.get(split0).size() + ": ");
        for (IndexedSplit split1 : joinTable.get(split0)) {
          ret.append(split1.getIndex() + ", ");
        }
        ret.append(" ]\n");
      }
    }
    return ret.toString();
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
  public synchronized Pack obtainLastLevelPack(
      Set<IndexedSplit> staticCache, Set<IndexedSplit> dynamicCache, 
      long cacheSize) {
    LOG.info("Generating a last level pack");
    //no pack if no group.
    if ((groups == null) || (groups.isEmpty())) return null;

    //pack from the group with most local splits
    //select tasks from the largest join set.
    HashSet<IndexedSplit> cache = new HashSet<IndexedSplit>();
    cache.addAll(staticCache);
    cache.addAll(dynamicCache);
    int bestGroupIndex = chooseBestGroup(cache);
    if (bestGroupIndex == -1) return null;
    IndexedSplit chosenSplit = chooseLargestSet(bestGroupIndex, cache);

    //find a join set
    int numPackedTasks = 0;
    long sizeLeft;
    boolean finished = false;
    Pack newPack = new Pack();
    Set<IndexedSplit> newDynamicCache = new HashSet<IndexedSplit>();
    // use half of the cache for the join set.
    sizeLeft = cacheSize / 2;
    TreeSet<IndexedSplit> tmp = joinTable.get(chosenSplit);
    TreeSet<IndexedSplit> chosenSet = new TreeSet<IndexedSplit>();
    for(IndexedSplit split : tmp) {
      chosenSet.add(split);
      if (!staticCache.contains(split)) {
        newDynamicCache.add(split);
        sizeLeft -= split.size();
        if(sizeLeft < 0) break;
      }
    }

    //start packing
    sizeLeft += cacheSize / 2;
    List<IndexedSplit[]> deleteList = new ArrayList<IndexedSplit[]>();
    // obtain local tasks first
    for (IndexedSplit local : cache) {
      //only splits in the old dynamic cache will increase the size, 
      //static splits are local.
      if (dynamicCache.contains(local))
        if (sizeLeft - local.size() < 0) 
          continue;

      //grab all splits in chosenSet that are also in joinSet
      tmp = joinTable.get(local);
      if (tmp == null) continue;

      int count = 0;
      for (IndexedSplit split : chosenSet) {
        if (tmp.contains(split)) {
          addPairToPack(newPack, local, split);
          IndexedSplit[] toDelete = new IndexedSplit[2];
          toDelete[0] = local;
          toDelete[1] = split;
          deleteList.add(toDelete);
          numPackedTasks ++;
          count ++;
          if (numPackedTasks >= maxPackSize) {
            LOG.debug("Reached pack size: local packing");
            finished = true;
            break;
          }
        }
      }

      if (count != 0) {
        //at least one join split is chosen
        if (dynamicCache.contains(local)) {
          newDynamicCache.add(local);
          sizeLeft -= local.size();
        }
      }

      if (finished == true) break;
    }

    if (finished == true) {
      removePairs(deleteList);
      LOG.info("Pack size: " + newPack.size());
      return newPack;
    }

    // try all non-locals in the same group
    for (IndexedSplit any : groups.get(bestGroupIndex)) {

      //check cache size
      if ((!staticCache.contains(any)) &&
          (!newDynamicCache.contains(any))) {
        if (sizeLeft - any.size() < 0) {
          //size will exceed, do not try further.
          LOG.debug("Reached size limit, num cached splits: " + 
                    newDynamicCache.size());
          finished = true;
          break;
        }
      }
      
      //grab all splits in chosenSet that are also in joinSet
      tmp = joinTable.get(any);
      if (tmp == null)  continue;

      int count = 0;
      for (IndexedSplit split : chosenSet) {
        if (tmp.contains(split)) {
          addPairToPack(newPack, any, split);
          IndexedSplit[] toDelete = new IndexedSplit[2];
          toDelete[0] = any;
          toDelete[1] = split;
          deleteList.add(toDelete);
          numPackedTasks ++;
          count ++;
          if (numPackedTasks >= maxPackSize) {
            LOG.debug("Reached pack size: non-local packing");
            finished = true;
            break;
          }
        }
      }
      
      if (count != 0) {
        if ((!staticCache.contains(any)) &&
            (!newDynamicCache.contains(any))) {
          //we chose some of "any"'s join set
          //we should put "any" into the new cache
          newDynamicCache.add(any);
          sizeLeft -= any.size();
        }
      }

      if (finished == true) break;
    }

    removePairs(deleteList);
    LOG.info("Pack size: " + newPack.size());
    return newPack;
  }

  private int chooseBestGroup(Set<IndexedSplit> cache) {
    if ((groupCache == null) || (groupCache.isEmpty())) return -1;

    Map<Integer, Integer> groupForlocals = new HashMap<Integer, Integer>();

    for (IndexedSplit split : cache) {
      Integer groupIndex = groupCache.get(split);
      if (groupIndex == null) continue;
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

    //local is in none of the group
    //just pick a group
    if (bestGroupIndex == -1) {
      bestGroupIndex = groups.size() - 1;
    }

    return bestGroupIndex;
  }
  
  //choose the largest join set in a group
  private IndexedSplit chooseLargestSet(int groupIndex, 
                                        Set<IndexedSplit> cache) {
    int largest = 0;
    IndexedSplit chosenSplit = null;
    HashSet<IndexedSplit> group = groups.get(groupIndex);
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

  /**
   * Obtain subpack
   *
   * For memory level cache, obtained from a pack.
   */
  public Pack obtainSubpack(Pack pack, long cacheSize) {
    LOG.info("Generating a sub pack\n");

    if ((pack == null) || (pack.isEmpty())) return null;

    Pack subPack = new Pack();

    //choose largest set
    int largest = 0;
    TreeSet<IndexedSplit> largestSet = null;
    for(IndexedSplit split : pack.keySet()) {
      TreeSet<IndexedSplit> set = pack.get(split);
      if ((largestSet == null) ||
          (set.size() > largestSet.size())) {
        largest = set.size();
        largestSet = set;
      }
    }

    if (largestSet == null) return null;

    //choose a sub set to fill half cache
    long sizeLeft = cacheSize / 2;
    TreeSet<IndexedSplit> chosenSet = new TreeSet<IndexedSplit>();
    for(IndexedSplit split : largestSet) {
      if (sizeLeft - split.size() < 0) break;
      chosenSet.add(split);
      sizeLeft -= split.size();
    }


    //choose corresponding tasks
    boolean finished = false;
    sizeLeft += cacheSize / 2;
    List<IndexedSplit[]> deleteList = new ArrayList<IndexedSplit[]>();
    Set<IndexedSplit> cachedSet = new HashSet<IndexedSplit>();
    for (IndexedSplit split : pack.keySet()) {
      TreeSet<IndexedSplit> set = pack.get(split);
      for (IndexedSplit joinSplit : set) {

        if (chosenSet.contains(joinSplit)) {
          //the first time split appears we should check the size
          //and add it to the cachedSet.
          if (!cachedSet.contains(split)) {
            if (sizeLeft - split.size() < 0) {
              finished = true;
              break;
            }
            cachedSet.add(split);
            sizeLeft -= split.size();
          }

//          LOG.debug("add to memory: " + split.getIndex());
//          LOG.debug("sizeLeft: " + sizeLeft);
//          LOG.debug("split size: " + split.size());

          addPairToPack(subPack, split, joinSplit);
          IndexedSplit[] toDelete = new IndexedSplit[2];
          toDelete[0] = split;
          toDelete[1] = joinSplit;
          deleteList.add(toDelete);
        }
      }
      if (finished) break;
    }

    for (IndexedSplit[] splitPair : deleteList) {
      removePairFromPack(pack, splitPair[0], splitPair[1]);
    }
    return subPack;
  }

  private static void addPairToPack(Pack pack, 
                                    IndexedSplit split0, IndexedSplit split1) {
    TreeSet<IndexedSplit> set = pack.get(split0);
    if (set == null) {
      set = new TreeSet<IndexedSplit>();
      pack.put(split0, set);
    }
    set.add(split1);
  }

  private static void removePairFromPack(
      Pack pack, IndexedSplit split0, IndexedSplit split1) {
    if (pack == null) return;
    TreeSet<IndexedSplit> set = pack.get(split0);
    if (set == null) return;
    set.remove(split1);
    if (set.isEmpty()) pack.remove(split0);
  }

  /**
   * Update members when move pairs of splits to pack This tries to maintain
   * the following rules.
   *
   * 1. a split is a key in joinTable if and only if the join set is non-empty;
   * 2. split has no order, (split0, split1), (split1, split0) * will both
   * appear or neither in joinTable; 
   * 3. a split is in groups if and only if it is a key in joinTable; 
   * 4. a split is a key in groupCache if and only if it is in groups.
   */
  private void removePairs(List<IndexedSplit[]> splitList) {
    for(IndexedSplit[] pair : splitList) {
      LOG.info("remove " + pair[0].getIndex() + " , " + pair[1].getIndex());
      IndexedSplit split0 = pair[0];
      IndexedSplit split1 = pair[1];
      TreeSet<IndexedSplit> tmpSet;

      tmpSet = joinTable.get(split0);
      //we can simply return from here if tmpSet is null
      //because splits appear in pair.
      if (tmpSet == null) return;
      tmpSet.remove(split1);
      clearSplitIfEmpty(split0);

      tmpSet = joinTable.get(split1);
      if (tmpSet == null) return;
      tmpSet.remove(split0);
      clearSplitIfEmpty(split1);
    }
  }

  private void clearSplitIfEmpty(IndexedSplit split) {
      TreeSet<IndexedSplit> tmpSet;
      tmpSet = joinTable.get(split);
      if (tmpSet.isEmpty()) {
        joinTable.remove(split);
        int groupIndex = groupCache.get(split);
        HashSet<IndexedSplit> group = groups.get(groupIndex);
        group.remove(split);
        if (group.isEmpty()) groups.remove(groupIndex);
        groupCache.remove(split);
      }
  }

  public boolean isFinished() {
    return (joinTable.isEmpty() && groups.isEmpty());
  }

}
