package org.apache.hadoop.map2;

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
import org.apache.hadoop.fs.Segments;
import org.apache.hadoop.fs.Segments.CoverInfo;

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
  private List<List<Segment>> groups;
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
      TreeSet<Segment> set = packMap.get(seg0);
      if (set == null) {
        set = new TreeSet<Segment>();
        packMap.put(seg0, set);
      }
      set.add(seg1);
    }

    //responsible to also remove duplicates
    public void removePair(Segment seg0, Segment seg1) {
      TreeSet<Segment> set = packMap.get(seg0);
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

    public Set<Segment> keySet() {
      return packMap.keySet();
    }

    public Segment[] getNext() {
      if ((packMap == null) || (packMap.isEmpty())) {
        return null;
      }
      if (current == null) {
        current = packMap.keySet().iterator().next();
      }

      TreeSet<Segment> set = packMap.get(current);
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
  private void initJoinTable(List<Segment[]> joinList) {
    
    joinTable = new HashMap<Segment, TreeSet<Segment>>();

    //add each pair to two sets.
    for (Segment[] pair : joinList) {
      for (int i = 0; i < 2; ++i) {
        TreeSet<Segment> set = joinTable.get(pair[i]);
        if (set == null) {
          set = new TreeSet<Segment>();
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
   * This method statically groups the Segments so that when fetching Map2Splits
   * from one group, the corresponding size of segment set will be small. A
   * dynamic packing strategy will be applied to each group according to the
   * cache status of each node.
   */
  private void initGroups() {
    groups = new ArrayList<ArrayList<Segment>>();
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
          groups.get(i).add(seg);
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
      ArrayList<Segment> newGroup = new ArrayList<Segment>();
      newGroup.add(seg);
      groups.add(newGroup);
      largestCache.put(groups.size() - 1, segJoinSet);
    } 

    //sort each group for future search
    for (List<Segment> group : groups) {
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
    for(Segment seg : smaller) {
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
  public synchronized Pack obtainLastLevelPack(Segments staticCache, 
                                               Segments dynamicCache, 
                                               long cacheSize) {
    long start = System.currentTimeMillis();

    LOG.info("Generating a last level pack");
    //no pack if no group.
    if ((groups == null) || (groups.isEmpty())) return null;

    //select from the group with least local leftSize
    //select from the largest join set.
    Segments cache = new Segments(staticCache);
    cache.add(dynamicCache);
    List<CoverInfo> bestGroup = chooseGroup(cache, cacheSize);
    if (bestGroup == null) return null;
    Set<Segment> largestSet = getLargestSet(bestGroup, cacheSize);
    if (largestSet == null) return null;

    int numPackedTasks = 0;
    long sizeLeft;
    boolean finished = false;
    Pack newPack = new Pack();
    Set<Segment> chosenSegments = new HashSet<Segment>();
    float overCacheFactor = 
        conf.getFloat("mapred.map2.taskPacker.overCacheFactor", 0.8);
    long usableCacheSize = cacheSize * overCacheFactor;
    sizeLeft = usableCacheSize / 2;
    //actually select form the join set
    // use a portion of the cache for the join set.
    Set<Segment> chosenJoinSet = new TreeSet<Segment>();
    for(Segment seg : largestSet) {
      chosenJoinSet.add(seg);
      CoverInfo info = staticCache.cover(seg);
      sizeLeft -= info.leftSize;
      chosenSegments.add(seg);
      if (sizeLeft < 0) break;
    }
    if (chosenSet.isEmpty()) return null;

    //start packing
    sizeLeft += usableCacheSize / 2;
    List<Segment[]> deleteList = new ArrayList<Segment[]>();

    LOG.info("Start packing");
    //since the cover info in bestGroup is already ordered according to the
    //locality, we can simply pop them out.
    for (CoverInfo info : bestGroup) {
      //we need to know the if static cache can cover this segment
      CoverInfo staticInfo = staticCache.cover(info.segment);

      if (!chosenSegments.contains(info.segment))
        if (sizeLeft - staticInfo.leftSize < 0)
          continue;

      Set<Segment> join = joinTable.get(info.segment);
      if (join == null) continue;

      int count = 0;
      for (Segment seg : chosenJoinSet) {
        if (join.contains(seg)) {
          newPack.addPair(info.segment, seg);
          Segment[] toDelete = new Segment[2];
          toDelete[0] = info.segment;
          toDelete[1] = seg;
          deleteList.add(toDelete);
          numPackedTasks ++;
          count ++;
          if (numPackedTasks >= maxPackSize) {
            LOG.debug("Reached pack size");
            finished = true;
            break;
          }
        }
      }

      if (count != 0) {
        //at least one join segment is chosen
        if (!chosenSegments.contains(info.segment)) {
          chosenSegments.add(info.segment);
          sizeLeft -= staticInfo.leftSize;
        }
      }

      if (finished == true) break;
    }

    removePairs(deleteList);
    long end = System.currentTimeMillis();
    LOG.debug("Finished last level packing in " + (end - start) + " ms " );
    return newPack;
  }

  /**
   * Try to cover each group with cache. 
   * 
   * Find the group with most segments within the required size.
   */
  private List<CoverInfo> chooseGroup(Segments cache, long size) {
    if (groups == null || groups.isEmpty()) return null;

    List<List<CoverInfo>> allCoverInfo = new ArrayList<List<CoverInfo>>();

    for (List<Segment> group : groups) {
      List<CoverInfo> info = cache.cover(group);
      allCoverInfo.add(info);
    }

    int maxNum = 0;
    List<CoverInfo> optGroup = null;
    for (List<CoverInfo> groupInfo : allCoverInfo) {
      int num = 0;
      long left = size;
      for (CoverInfo info : groupInfo) {
        left -= info.leftSize;
        num ++;
        if (left < 0) break;
      }
      if (num > maxNum) {
        maxNum = num;
        optGroup = groupInfo;
      }
    }

    return optGroup;
  }
  
  //choose the largest join set in a group
  private Set<Segment> getLargestSet(List<CoverInfo> group, long size) {
    Set<Segment> chosenSet = null;
    int idx = 0;
    long left = size;
    while((left > 0) && (idx < group.size())) {
      CoverInfo info = group.get(idx);
      left -= info.leftSize;
      Set<Segment> currSet = joinTable.get(info.segment);
      if (currSet.size() > chosenSet.size()) {
        chosenSet = currSet;
      }
    }
    return chosenSet;
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
    TreeSet<Segment> largestSet = null;
    for(Segment seg : pack.keySet()) {
      TreeSet<Segment> set = pack.get(seg);
      if ((largestSet == null) ||
          (set.size() > largestSet.size())) {
        largestSet = set;
      }
    }

    if (largestSet == null) return null;

    Set<Segment> chosenSegments = new HashSet<Segment>();
    //choose a sub set to fill half cache
    float overCacheFactor = 
        conf.getFloat("mapred.map2.taskPacker.overCacheFactor", 0.8);
    long usableCacheSize = cacheSize * overCacheFactor;
    long sizeLeft = usableCacheSize / 2;
    TreeSet<Segment> chosenSet = new TreeSet<Segment>();
    for(Segment seg : largestSet) {
      if (sizeLeft - seg.getLength() < 0) break;
      chosenSet.add(seg);
      chosenSegments.add(seg);
      sizeLeft -= seg.getLength();
    }


    //choose corresponding tasks
    boolean finished = false;
    sizeLeft += usableCacheSize / 2;
    List<Segment[]> deleteList = new ArrayList<Segment[]>();
    for (Segment seg : pack.keySet()) {
      TreeSet<Segment> set = pack.get(seg);
      for (Segment joinSeg : set) {

        if (chosenSet.contains(joinSeg)) {
          //the first time split appears we should check the size
          //and add it to the cachedSet.
          if (!chosenSegments.contains(split)) {
            if (sizeLeft - seg.getLength() < 0) {
              finished = true;
              break;
            }
            chosenSegments.add(seg);
            sizeLeft -= seg.getLength();
          }

//          LOG.debug("add to memory: " + split.getIndex());
//          LOG.debug("sizeLeft: " + sizeLeft);
//          LOG.debug("split size: " + split.size());

          subPack.addPair(seg, joinSeg);
          Segment[] toDelete = new Segment[2];
          toDelete[0] = seg;
          toDelete[1] = joinSeg;
          deleteList.add(toDelete);
        }
      }
      if (finished) break;
    }

    for (Segment[] segPair : deleteList) {
      pack.removePair(segPair[0], segPair[1]);
    }

    long end = System.currentTimeMillis();
    LOG.info("Finish sub-packing in " + (end - start) + " ms.");
    return subPack;
  }

  /**
   * Update members when move pairs of splits to pack This tries to maintain
   * the following rules.
   *
   * 1. a seg is a key in joinTable if and only if the join set is non-empty;
   * 2. seg has no order, (seg, split1), (seg, split0) * will both
   * appear or neither in joinTable; 
   * 3. a seg is in groups if and only if it is a key in joinTable; 
   */
  private void removePairs(List<Segment[]> pairList) {
    for(Segment[] pair : pairList) {
      Segment seg0 = pair[0];
      Segment seg1 = pair[1];
      TreeSet<Segment> tmpSet;

      tmpSet = joinTable.get(seg0);
      //we can simply continue from here if tmpSet is null
      //because splits appear in pair.
      if (tmpSet == null) continue;
      tmpSet.remove(seg1);
      clearSegIfEmpty(seg0);

      tmpSet = joinTable.get(seg1);
      if (tmpSet == null) continue;
      tmpSet.remove(seg0);
      clearSplitIfEmpty(seg1);
    }
  }

  private void clearSegIfEmpty(Segment seg) {
      TreeSet<Segment> tmpSet;
      tmpSet = joinTable.get(seg);
      if (tmpSet.isEmpty()) {
        joinTable.remove(seg);
        List<Segment> group = null;
        int i;
        for (i = 0; i < groups.size(); ++i) {
          group = groups.get(i);
          if (group.remove(seg)) break;
        }
        if (group.isEmpty()) groups.remove(i);
      }
  }

  public boolean isFinished() {
    return (joinTable.isEmpty() && groups.isEmpty());
  }

}
