#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include <list>
#include <map>
#include <vector>

#include "Schedule.h"
#include "LogicArray.h"

/*!\file MapReduce.h
 * \brief This file represents the implementation of a map reduce framework. We
 * add a LoadHint class and related functions that interactive with the original
 * framework classes and functions to achieve a more IO effective map reduce
 * framework.  The LoadHint class is added so that the underlying framework
 * is aware of the overlapping relationship among data chunks touched by each
 * map-reduce job.
 *
 * To Do:
 * The interface is not compatible with the original framwork, e.g.
 * Mapper(..., std::vector<LogicArrayBlock>). Need to be fixed.
 */

/*!\class LoadHint
 * \brief Emit a hint of IO for a (key, value) pair to the underlying framework.
 */
template<typename MapKey, typename MapValue>
class LoadHint {
 public:
  /*** typedef ***/
  typedef KeyType MapKey;
  typedef ValType MapValue;

  /*** member function ***/
  /* operator()
   * Should be defined by user. MapReduceJob is the current job the mapper in
   * which provide an interface function emit_loadhint(). Mappers can use
   * this function to emit an intermediate (key, value) to the underlying
   * framework.
   *
   * Example:
   * The definition of operator() in a matrix multiplication application:
   * void operator() {MapReduceJob& job, const KeyType& key, const ValType& val)
   * {
   *   std::vector<LogicArrayBlock> data_list;
   *   std::vector<Range> range_list;
   *   
   *   range_list.push(Range(val.A_column_idx));
   *   range_list.push(logic_array::EntireDim);
   *   LogicArrayBlock Ablock(A, range_list);     //A[i,:]
   *
   *   range_list.clear();
   *   range_list.push(logic_array::EntireDim);
   *   range_list.push(Range(val.B_row_idx));
   *   LogicArrayBlock Bblock(B, range_list);     //B[:,j]
   *
   *   range_list.clear();
   *   range_list.push(Range(val.A_column_idx);
   *   range_list.push(Range(val.B_row_idx));
   *   LogicArrayBlock Cblock(C, range_list));    //C[i,j]
   *
   *   data_list.push_back(Ablock);
   *   data_list.push_back(Bblock);
   *   data_list.push_back(CBlock);
   *   job.emitLoadHint(key, val, data_list);
   * }
   */
  template<typename MapReduceJob>
  void operator() (MapReduceJob& job, const KeyType& key, const ValType& val);
};

/*!\class Mapper
 * \brief Mapper class base. User defined mappers inherites from this class.
 * Our framework adds a special field(and related methods) called 
 */
template<typename MapKey, typename MapValue>
class Mapper {
 public:
  /*** typedef ***/
  typedef KeyType MapKey;
  typedef ValType MapValue;

  /*** member function ***/
  /* operator()
   * Should be defined by user. MapReduceJob is the current job the mapper in
   * which provide an interface function emitMapIntermediate(). Mappers can use
   * this function to emit an intermediate (key, value) to the underlying
   * framework.
   */
  template<typename MapReduceJob>
  void operator()(MapReduceJob& job, const KeyType& key, const ValType& val, std::vector<LogicArrayBlock> data);
};

/*!\class Reducer
 */
template<typename ReduceKey, typename ReduceValue>
class Reducer {
  /*** typedef ***/
  typedef KeyType MapKey;
  typedef ValType MapValue;
};

/*!\class MapReduceJob
 * \brief Simplified interface for a map-reduce job. Just a demonstration.
 * Mapper, Reducer and LoadHint are specified by template.
 * To run a map-reduce job insert (key, value) pairs and run
 *
 * To Do:
 * Based on real implementation, we need to answer: how to distributively share
 * the work_list and fetch works.
 */
template<typename Mapper, typename Reducer, typename LoadHint>
class MapReduceJob {
 public:
  /*** member function ***/
  /* insert()
   * Insert (key, value) pairs for mapper.
   */
  void insert(Mapper::KeyType, Mapper::ValType);

  /* emitLoadHint()
   * Emit a load hint to the job. Used by LoadHint.
   */
  void emitLoadHint(Mapper::KeyType, Mapper::ValType, std::vector<LogicArrayBlock>);

  /* emitMapIntermediate()
   * Emit a map result to the job. Used by Mapper.
   */
  void emitMapIntermediate(Reducer::KeyType, Reducer::ValType);

  /* run()
   */
  void run();
 private:
  /*** member variables ***/
  //mapper set including (key, value) pairs and load hints
  std::map< std::pair<Mapper::KeyType, Mapper::ValType>, std::vector<LogicArrayBlock> > map_set;
  //reduce set
  std::map< Reducer::KeyType, Reducer::ValType > reduce_set;
  //mapper worklist
  std::list< std::pair<Mapper::KeyType, Mapper::ValType> > work_list;

  /*** helper functions ***/
  void prepare();
};

/*** \impl MapReduceJob ***/
/* insert() */
template<typename Mapper, typename Reducer, typename LoadHint>
void MapReduceJob::insert(const Mapper::KeyType& key, const Mapper::ValType& val) {
  std::pair<Mapper::KeyType, Mapper::ValType> pair(key, val);
  map_set[pair] = std::vector<LogicArrayBlock>();
}

/* emitLoadHint() */
template<typename Mapper, typename Reducer, typename LoadHint>
void MapReduceJob::emitLoadHint(Mapper::KeyType& key, Reducer::ValType& val, 
                                std::vector<LogicArrayBlock> hint) {
  std::pair<Mapper::KeyType, Mapper::ValType> pair(key, val);
  map_set[pair] = hint;
}

/* emitMapIntermediate() */
template<typename Mapper, typename Reducer, typename LoadHint>
void MapReduceJob::emitMapIntermediate(const Reducer::KeyType& key, const Reducer::ValType& val) {
  reduce_set[key] = value;
}

/* prepare() */
template<typename Mapper, typename Reducer, typename LoadHint>
void MapReduceJob::prepare() {
  //Analyze the overlap condition 
  sched::genWorkList(map_set, work_list);
}

/* run() */
template<typename Mapper, typename Reducer, typename LoadHint>
void MapReduceJob::run() {
  //here we need to modify the framework.
  //before run mapper, we need to prepare the work_list
  parepare();
  //run mapper
  while (!work_list.empty()) {
    std::pair<Mapper::KeyType, Mapper::ValType> work = work_list.front();
    work_list.pop();
    std::vector<LogicArrayBlock> blocks = map_set[work];
    for (std::vector<LogicArrayBlock>::iterator it = blocks.begin();
         it != blocks.end(); ++it) {
      //load
      (*it).load();
    }
    //actually run the mapper
    Mapper()(work.first, work.second, blocks);
  }

  // reduce part does not change currently
  // run reduce
  // ....
}

#endif /* MAPREDUCE_H_ */
