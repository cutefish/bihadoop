/* MapTaskPacker.java
 * Handles packing of map tasks.
 */

class MapTaskPacker {
  //implement a new inputSplit which can be ordered.
  
  //A data structure that divide the set input0 to a list of subsets.
  //If the task scheduling matrix is a dense one, there will be only one subset.
  //If it is a sparse matrix, inside each subsets, the number of elements of set input1 related
  //to each input0 element shares at least 80%.
  List< Map<InputSplit, TreeSet<InputSplit> > > unpackedTaskCache;

  //this is used for debug
  void readJobMetaFile(Path jobTaskFile) {
    //createUnpackedTaskCache(maps);
  }

  //Create the unpackedTaskCache
  //1. if !isSparse, put all tasks into one entry of list
  //2. if isSparse, tasks are devided into multiple sets where
  //inside each set, the second input set shares a lot of similarity.
  void createUnpackedTaskCache(TaskInProgress[] maps, boolean isSparse) {
  }

  //Obtain a pack of tasks according to the current status of TaskTracker
  List<TaskInProgress> obtainPack(TaskTrackerStatus tts) {
    //The packs are constructed as following:
    //1. calculate the suitable number of a pack, such that:
    //   (1) the total number of input should not exceed the size of
    //   cache(either disk cache or memroy cache)
    //   (2) the total number of tasks should not exceed the cluster average
    //   number of tasks.
    //2. schedule tasks that could have already been on the node
    //3. select an on-node split, select other on node splits that in
    //the same bin, try to obtain enough tasks.
    //4. select the same on-node split, select arbitrary splits
    //inside the same bin, try to obtain enough tasks.
    //5. select an arbitrary bin and try to obtain enough tasks.
  }

}
