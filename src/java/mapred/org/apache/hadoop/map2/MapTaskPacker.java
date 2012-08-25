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
import org.apache.hadoop.core.Configuration;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.SegmentUtils;

/** 
 * Handles packing of map tasks.
 *
 * The goal of packing is to minimize the footprint(size of segment set) for
 * each pack of task. Further, the locality of segments between tasks and jobs
 * are considered. This results into a two-step scheduling: a static grouping
 * according to the sets of segments among tasks; and a dynamic packing
 * considering locality. The scheduling heuristic makes an assumption that the
 * overlap patterns between segments are regular. That is to say, segments 
 * either have an inclusion relationship or have no overlap at all.
 */
public class MapTaskPacker {

  private static final Log LOG = LogFactory.getLog(MapTaskPacker.class);

  private Configuration conf;
  private static final float ACCEPT_OVERLAP_RATIO = 0.8f;
  private int totalNumMaps = 0;
  private int clusterSize = 0;
  private int maxPackSize = 0;
  private LinkedList<LinkedList<Segment>> groups;
  private Map<Segment, TreeSet<Segment>> joinTable;

  public MapTaskPacker(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Pack of tasks(Segment pairs)
   */
  public static class Pack {
    private Map<Segment, TreeSet<Segment>> packMap;
    private Segment current = null;

    public Pack() {
      this.packMap = new HashMap<Segment, TreeSet<Segment>>();
    }

    public TreeSet<Segment> get(Segment seg) {
      return packMap.get(seg);
    }

    public void put(Segment seg, TreeSet<Segment> set) {
      packMap.put(seg, set);
    }

    public void remove(Segment seg) {
      packMap.remove(seg);
    }

    //possibly adding duplicates: a->b and b->a
    public void addPair(Segment seg0, Segment seg1) {
      TreeSet<IndexedSplit> set = packMap.get(seg0);
      if (set == null) {
        set = new TreeSet<IndexedSplit>();
        packMap.put(seg0, set);
      }
      set.add(seg1);
    }

    //responsible to also remove duplicates
    public void removePair(Segment seg0, Segment seg1) {
      TreeSet<IndexedSplit> set = packMap.get(seg0);
      if (set != null) {
        set.remove(seg1);
        if (set.isEmpty()) packMap.remove(seg0);
      }
      set = packMap.get(seg1);
      if (set != null) {
        set.remove(seg0);
        if (set.isEmpty()) packMap.remove(seg1);
      }
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

    public Segment[] getNext() {
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
      Segment joinSegment = set.first();
      Segment[] nextSplit = new Segment[2];
      nextSplit[0] = current;
      nextSplit[1] = joinSegment;
      removePair(current, joinSegment);
      return nextSplit;
    }

    public String toString() {
      StringBuilder ret = new StringBuilder();
      for (Iterator<Map.Entry<Segment, TreeSet<Segment>>>
           I = packMap.entrySet().iterator(); I.hasNext();) {
        Map.Entry<Segment, TreeSet<Segment>> entry = I.next();
        Segment seg0 = entry.getKey();
        ret.append(seg0.toString() + " [");
        TreeSet<Segment> joinSet = entry.getValue();
        for (Segment seg1 : joinSet) {
          ret.append(seg1.toString() + ", ");
        }
        ret.append("]\n");
      }
      return ret.toString();
    }
  }

  /**
   * Initialize MapTaskPacker from a list of Segment[2].
   *
   * Segments in each split are not ordered, that is, we do not distinguish 
   * segment[0] and segment[1]
   */
  public void init(List<Segment[]> segList, int clusterSize) {

    long start = System.currentTimeMillis();

    this.clusterSize = clusterSize;
    initJoinTable(segList);
    initGroups();
    maxPackSize = totalNumMaps / clusterSize + 
        ((totalNumMaps % clusterSize == 0) ? 0 : 1);

    long end = System.currentTimeMillis();

    LOG.info("Number of Groups size: " + groups.size());
    LOG.info("Finished initializing for job in " + (end - start) + " ms.");
  }

  /**
   * Initialize the joinTable.
   *
   * JoinTable is a table to describe the join set of every segment.
   */
  private void initJoinTable(List<Segment[]> segList) {
    joinTable = new HashMap<Segment, TreeSet<Segment>>();

    //add each pair to two sets.
    for (Segment[] pair : segList) {
      for (int i = 0; i < 2; ++i) {
        TreeSet<Segment> set = joinTable.get(pair[i]);
        if (set == null) {
          set = new TreeSet<IndexedSplit>();
          joinTable.put(pair[i], set);
        }
        set.add(pair[1 - i]);
        totalNumMaps ++;
      }
    }
    
    totalNumMaps /= 2;
    LOG.info("Total Number of Tasks: " + totalNumMaps);
  }

  /**
   * Initialize the group from joinTable.
   *
   * This method statically groups the Segments so that inside each group
   * the size of segment set for each map2split is small. A dynamic packing
   * strategy will be applied to each group according to the cache status of
   * each node.
   */
  private void initGroups() {
    groups = new LinkedList<LinkedList<Segment>>();

    //keep a cache for the largest set in a group
    Map<Integer, TreeSet<Segment>> largestCache = 
        new HashMap<Integer, TreeSet<Segment>>();

    //foreach in the joinTable
    for (Iterator<Map.Entry<Segment, TreeSet<Segment>>> 
        I = joinTable.entrySet().iterator(); I.hasNext();) {
      Map.Entry<Segment, TreeSet<Segment>> entry = I.next();
      Segment seg = entry.getKey();
      TreeSet<Segment> segJoinSet = entry.getValue();

      boolean groupFound = false;
      //foreach in the group list
      for (int i = 0; i < groups.size(); ++i) {
        TreeSet<Segment> largest = largestCache.get(i);
        assert (largest != null) : ("Largest entry not constructed");
        if (shouldContain(largest, segJoinSet)) {
          groups.get(i).add(split);
          //update largest cache
          if (segJoinSet.size() > largest.size()) {
            largestCache.put(i, segJoinSet);
          }
          groupFound = true;
          break;
        }
      }

      if (groupFound) continue;

      //no group contains this entry
      LinkedList<Segment> newGroup = new LinkedList<Segment>();
      newGroup.add(seg);
      groups.add(newGroup);
      largestCache.put(groups.size() - 1, segJoinSet);
    } 

    //sort each group for future search
    for (LinkedList<Segment> group : groups) {
      Collections.sort(group);
    }
  }

  private boolean shouldContain(Set<Segment> first,
                                Set<Segment> second) {
    Set<Segment> smaller;
    Set<Segment> larger;
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
    for (Iterator<Map.Entry<Segment, TreeSet<Segment>>>
         I = joinTable.entrySet().iterator(); I.hasNext();) {
      Map.Entry<Segment, TreeSet<Segment>> entry = I.next();
      Segment seg0 = entry.getKey();
      ret.append(seg0.toString() + " [");
      TreeSet<Segment> joinSet = entry.getValue();
      ret.append(joinSet.size() + ": ");
      for (Segment seg1 : joinSet) {
        ret.append(seg1.toString() + ", ");
      }
      ret.append("]\n");
    }
    return ret.toString();
  }

  //for debug
  public String groupsToString() {
    StringBuilder ret = new StringBuilder();
    ret.append("groupNo " + groups.size() + '\n');
    for (int i : groups.keySet()) {
      ret.append("group " + i + '\n');
      List<Segment> group = groups.get(i);
      for (Segment seg0 : group) {
        ret.append(seg0.toString() + " [");
        ret.append(joinTable.get(seg0).size() + ": ");
        for (Segment seg1 : joinTable.get(seg0)) {
          ret.append(seg1.toString() + ", ");
        }
        ret.append(" ]\n");
      }
    }
    return ret.toString();
  }

  //for debug
  public int numGroups() {
    return groups.size();
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
  public synchronized Pack obtainLastLevelPack(List<Segment> staticCache, 
                                               List<Segment> dynamicCache, 
                                               long cacheSize) {
    long start = System.currentTimeMillis();

    LOG.info("Generating a last level pack");
    //no pack if no group.
    if ((groups == null) || (groups.isEmpty())) return null;

    //pack from the group with most local splits
    //select tasks from the largest join set.
    Collections.sort(staticCache);
    Collections.sort(dynamicCache);
    List<Segment> cache = sortedMerge(staticCache, dynamicCache);
    int bestGroupIndex = chooseGroup(cache);
    if (bestGroupIndex == -1) return null;
    Segment chosenSegment = getLeftMostSegment(bestGroupIndex, cache);

    //find a join set
    int numPackedTasks = 0;
    long sizeLeft;
    boolean finished = false;
    Pack newPack = new Pack();
    Set<Segment> newDynamicCache = new HashSet<Segment>();
    // use a portion of the cache for the join set.
    float overCacheFactor = 
        conf.getFloat("mapred.map2.taskPacker.overCacheFactor", 0.8);
    long usableCacheSize = cacheSize * overCacheFactor;
    sizeLeft = usableCacheSize / 2;
    TreeSet<Segment> tmp = joinTable.get(chosenSplit);
    TreeSet<Segment> chosenSet = new TreeSet<Segment>();
    for(Segment seg : tmp) {
      chosenSet.add(seg);
      if (!staticCache.contains(split)) {
        newDynamicCache.add(split);
        sizeLeft -= split.size();
        if(sizeLeft < 0) break;
      }
    }

    //start packing
    sizeLeft += usableCacheSize / 2;
    List<IndexedSplit[]> deleteList = new ArrayList<IndexedSplit[]>();

    LOG.info("Start packing");
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
          newPack.addPair(local, split);
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
          newPack.addPair(any, split);
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

    long end = System.currentTimeMillis();

    LOG.debug("Finished last level packing in " + (end - start) + " ms " );

    return newPack;
  }

  private List<Segment> sortedMerge(List<Segment> seg0,
                                    List<Segment> seg1) {
    List<Segment> ret = new LinkedList<Segment>();
    int idx0 = 0, idx1 = 0;
    int num = 0;
    while (idx0 < seg0.size() &&
           idx1 < seg1.size()) {
      if (seg0.get(idx0).compareTo(seg1.get(idx1)) <= 0) {
        ret.add(seg0.get(idx0));
        idx0 ++;
      }
      else {
        ret.add(seg1.get(idx1));
        idx1 ++;
      }
      num ++;
    }

    if (idx0 == seg0.size()) {
      ret.addAll(seg1.subList(idx1, seg1.size()));
    }
    else {
      ret.addAll(seg0.subList(idx0, seg0.size()));
    }
    return ret;
  }

  /**
   * Compare cache with group indices to choose a group.
   *
   * Halt when selects a group of 0.8 match or the best.
   */
  private int chooseGroup(List<Segment> cache) {
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
      if ((groups != null) && (!groups.isEmpty())) {
        bestGroupIndex = groups.keySet().iterator().next();
      }
    }

    return bestGroupIndex;
  }
  
  //choose the largest join set in a group
  private Segment getLeftMostSegment(int groupIndex, 
                                     Set<IndexedSplit> cache) {
    int largest = 0;
    IndexedSplit chosenSplit = null;
    HashSet<IndexedSplit> group = groups.get(groupIndex);
    if (group == null) {
      LOG.info("no group: " + groupIndex);
      LOG.info(groupsToString());
    }
    for (IndexedSplit split : cache) {
      if (group.contains(split)) {
        if (joinTable.get(split).size() > largest) {
          largest = joinTable.get(split).size();
          chosenSplit = split;
        }
      }
    }

    //this happens when no local is in a group
    //choose any split
    if (chosenSplit == null) {
      chosenSplit = group.iterator().next();
    }
    return chosenSplit;
  }

  private static long getSegListSize(List<Segment> list) {
    long size = 0;
    for (Segment seg : list) {
      size += seg.getLength();
    }
    return size;
  }

  /**
   * Get the length of contingous data that is common to both list.
   *
   * Assumption:
   * (1) both lists are sorted.
   * (2) segments in each list have minimum overlap.
   */
  private static long getCommonLength(List<Segment> list0,
                                      List<Segment> list1) {
    List<Segment> smaller, larger;
    if (list0.size() < list1.size()) {
      smaller = list0;
      larger = list1;
    }
    else {
      smaller = list1;
      larger = list0;
    }

    int curr = 0;
    for (int i = 0; i < smaller.size(); ++i) {
      //search the position of elements from list0 in list1
      int idx = Collections.binarySearch(larger, smaller.get(i));
      curr = (idx >= 0) ? idx : -(idx + 1);
      while(true) {
      }
    }
  }

  private static boolean containsSegment(List<Segment> list, Segment key) {
  }

  /**
   * Obtain subpack
   *
   * For memory level cache, obtained from a pack.
   */
  public Pack obtainSubpack(Pack pack, long cacheSize) {
    long start = System.currentTimeMillis();

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

          subPack.addPair(split, joinSplit);
          IndexedSplit[] toDelete = new IndexedSplit[2];
          toDelete[0] = split;
          toDelete[1] = joinSplit;
          deleteList.add(toDelete);
        }
      }
      if (finished) break;
    }

    for (IndexedSplit[] splitPair : deleteList) {
      pack.removePair(splitPair[0], splitPair[1]);
    }

    long end = System.currentTimeMillis();
    LOG.info("Finish sub-packing in " + (end - start) + " ms.");
    return subPack;
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
      IndexedSplit split0 = pair[0];
      IndexedSplit split1 = pair[1];
      TreeSet<IndexedSplit> tmpSet;

      tmpSet = joinTable.get(split0);
      //we can simply continue from here if tmpSet is null
      //because splits appear in pair.
      if (tmpSet == null) continue;
      tmpSet.remove(split1);
      clearSplitIfEmpty(split0);

      tmpSet = joinTable.get(split1);
      if (tmpSet == null) continue;
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
