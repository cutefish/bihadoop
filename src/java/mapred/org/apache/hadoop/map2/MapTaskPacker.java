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
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Segments;
import org.apache.hadoop.fs.Segments.CoverInfo;

/** 
 * Handles packing of map tasks.
 *
 * The goal of packing is to minimize the footprint(while still load balance) for
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
  private List<List<Segment>> groups;
  private Map<Segment, TreeSet<Segment>> joinTable;
  private Map<Segment, Segment> coverMap;
  private int packRowSize;
  private int packColSize;

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
  public void init(List<Segment[]> segList,
                   Map<Segment, Segment> coverMap,
                   int packRowSize,
                   int packColSize) {

    this.coverMap = coverMap;
    this.packRowSize = packRowSize;
    this.packColSize = packColSize;

    long start = System.currentTimeMillis();

    initJoinTable(segList);
    initGroups();

    long end = System.currentTimeMillis();

    LOG.debug(groupsToString());

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
    groups = new ArrayList<List<Segment>>();
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
      if (larger.contains(seg)) {
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
    for (int i = 0; i < groups.size(); ++i) {
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
   * Heuristic:
   *
   * This heuristic has the following assumptions:
   * 1. Execution history is more important and optimal, the heuristic should
   * favor executed footprint. This means dynamic cache and perfect match has
   * more weight.
   * 2. The heuristic uses half of the capacity for each input set to get a
   * minal footprint
   */
  public synchronized Pack obtainLastLevelPack(Segments staticCache, 
                                               Segments dynamicCache, 
                                               long cacheSize, 
                                               int clusterSize) {
    int maxPackSize = totalNumMaps / clusterSize;
    maxPackSize = (maxPackSize == 0) ? 1 : maxPackSize;

    long start = System.currentTimeMillis();

    //no pack if no group.
    if ((groups == null) || (groups.isEmpty())) return null;

    LOG.debug("staticCache: " + staticCache);
    LOG.debug("dynamicCache: " + dynamicCache);

    //select a best group
    //favor dynamic cache.
    List<CoverInfo> bestGroup = null;
    if (dynamicCache.size() != 0) {
      bestGroup = chooseGroup(dynamicCache, cacheSize, false);
    }
    if (bestGroup == null) {
      bestGroup = chooseGroup(staticCache, cacheSize, true);
    }
    if (bestGroup == null) return null;
    LOG.debug("Best group: " + bestGroup.size() + 
              "[" + bestGroup.get(0).toString() + "]");

    //select the largest join set from the group
    Set<Segment> joinSetPool = getJoinSet(bestGroup, cacheSize);
    if (joinSetPool == null) return null;
    LOG.debug("JoinSetPool: " + joinSetPool.size());

    //start packing
    // This packing process tries to give a accurate estimation of how the large
    //the footprint is (assure it does not exceed the capacity).
    // Half of the capacity is used for each input set to heuristically minimize
    //the average footprint of each task.

    int squareSize = (int)Math.sqrt(maxPackSize);
    squareSize = (squareSize == 0) ? 1 : squareSize;
    int maxRowSize = Math.max(squareSize, maxPackSize / bestGroup.size());
    maxRowSize = Math.min(maxRowSize, joinSetPool.size());
    int maxColSize = maxPackSize / maxRowSize;

    /// choose a join set that fits in the half capacity
    long cap = cacheSize / 2;
    Set<Segment> rowSet = new TreeSet<Segment>();
    int size = (packRowSize == -1) ?  maxRowSize : packRowSize;
    int num = 0;
    for (Segment seg : joinSetPool) {
      if (num >= size) break;
      Segment cover = coverMap.get(seg);
      CoverInfo info = staticCache.cover(cover);
      if (cap - info.remainLen < 0) break;
      rowSet.add(seg);
      num ++;
      cap -= info.remainLen;
    }
    LOG.debug("rowSet size: " + rowSet.size() + " remain cap: " + cap);

    /// choose tasks from the group keys
    Pack newPack = new Pack();
    List<Segment[]> deleteList = new ArrayList<Segment[]>();
    cap += cacheSize / 2;
    size = (packColSize == -1) ?  maxColSize : packColSize;
    int colSize = 0;
    int packSize = 0;
    boolean finished = false;
    for (CoverInfo info : bestGroup) {
      LOG.debug("cover info:" + info);
      if (colSize >= size) break;

      //see if the join set has the segment we want
      boolean shouldPickThis = false;
      Set<Segment> join = joinTable.get(info.segment);
      for (Segment seg : rowSet) {
        if (join.contains(seg)) {
          shouldPickThis = true;
        }
      }
      if (!shouldPickThis) continue;

      //see if the size fit
      Segment cover = coverMap.get(info.segment);
      CoverInfo staticInfo = staticCache.cover(cover);
      if (cap - staticInfo.remainLen < 0) continue;

      //we should pick this segment
      cap -= staticInfo.remainLen;
      colSize ++;

      //add all pairs
      for (Segment seg : rowSet) {
        if (join.contains(seg)) {
          newPack.addPair(info.segment, seg);
          Segment[] toDelete = new Segment[2];
          toDelete[0] = info.segment;
          toDelete[1] = seg;
          deleteList.add(toDelete);
          packSize ++;
          if (packSize >= maxPackSize) {
            LOG.info("Reached pack size");
            finished = true;
            break;
          }
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
  private List<CoverInfo> chooseGroup(Segments cache, 
                                      long size, boolean chooseAny) {
    if (groups == null || groups.isEmpty()) return null;

    List<List<CoverInfo>> allCoverInfo = new ArrayList<List<CoverInfo>>();

    for (int i = 0; i < groups.size(); ++i) {
      List<Segment> group = groups.get(i);
      List<CoverInfo> info = cache.cover(group);
      if ((info != null) && (!info.isEmpty())) {
        LOG.debug("group: " + i + 
                  " info[0]: " + info.get(0).segment.toString() + 
                  " size: " + info.get(0).remainLen);
        allCoverInfo.add(info);
      }
    }

    if (allCoverInfo.isEmpty()) {
      //this means there is no cache just pick one group and construct the
      //info.
      if (chooseAny) {
        List<Segment> group = groups.get(0);
        List<CoverInfo> info = new ArrayList<CoverInfo>();
        for (Segment seg : group) {
          info.add(new CoverInfo(seg.getLength(), seg));
        }
        return info;
      }
      else {
        return null;
      }
    }

    List<CoverInfo> optGroup = null;
    int max = 0;
    //first count the occurrence of perfect match.
    for (List<CoverInfo> groupInfo : allCoverInfo) {
      int num = 0;
      for (CoverInfo info : groupInfo) {
        if (info.remainLen == 0) {
          num ++;
        }
      }
      //choose the group where the most zero occurs
      if (num > max) {
        max = num;
        optGroup = groupInfo;
      }
    }

    if (optGroup != null) return optGroup;

    //no group has perfect cover, use size.
    //half of the size can be used for the group keys.
    max = 0;
    for (List<CoverInfo> groupInfo : allCoverInfo) {
      int num = 0;
      long sizeLeft = size / 2;
      for (CoverInfo info : groupInfo) {
        if (sizeLeft - info.remainLen < 0) break;
        sizeLeft -= info.remainLen;
        num ++;
      }
      //choose the group where covers the most
      if (num > max) {
        max = num;
        optGroup = groupInfo;
      }
    }

    return optGroup;
  }
  
  //choose the largest join set in a group
  private Set<Segment> getJoinSet(List<CoverInfo> group, long size) {
    Set<Segment> chosenSet = null;
    int max = 0;
    for (CoverInfo info : group) {
      Set<Segment> set = joinTable.get(info.segment);
      if (set.size() > max) {
        max = set.size();
        chosenSet = set;
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


    if ((pack == null) || (pack.isEmpty())) return null;

    LOG.debug("Generating a sub pack\n");

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
        conf.getFloat("mapred.map2.taskPacker.overCacheFactor", 0.8f);
    long usableCacheSize = (long)(cacheSize * overCacheFactor);
    long sizeLeft = usableCacheSize / 2;
    TreeSet<Segment> chosenSet = new TreeSet<Segment>();
    for(Segment seg : largestSet) {
      if ((!chosenSet.isEmpty()) && 
          (sizeLeft - seg.getLength() < 0)) break;
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
          if (!chosenSegments.contains(seg)) {
            if (!subPack.isEmpty() && 
                (sizeLeft - seg.getLength() < 0)) {
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
    LOG.debug("Finish sub-packing in " + (end - start) + " ms.");
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
      clearSegIfEmpty(seg1);
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
