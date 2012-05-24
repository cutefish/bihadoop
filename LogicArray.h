#ifndef LOGICARRAY_H_
#define LOGICARRAY_H_

#include <vector>

#include "SmallVector.h"

/*!\file LogicArray.h
 * \brief This file implements a abstract LogicArray data structure. The
 * instances of LogicArray can be very large such that it is possible that no
 * single storage unit is large enough to store it. Methods are provided to load
 * a part of the LogicArray into local storage unit to access it.
 *
 * To Do: 
 * (1)Implement classes in a more efficient way.
 * (2)There might be a design flaw here. Loader and Dumper should be compatible
 * or otherwise dumped data will not be loaded again. Users might not be aware
 * of that?
 * (3)The user can access the logic array by operator(), however, this currently
 * can only return a void* so that it is actually not very convenient and
 * intuitive to for the user. This gives some disadvantage. However, this also
 * gives an advantage that the user will not use this method frequently. 
 * Instead they will further use the returned pointer so that we do not need to
 * care too much about the efficiency of this method.
 */

namespace logic_array {

static const EntireDim = -1;

} /* logic_array */

/*!\class Range
 * \brief Represent an unsigned interval [start(), end()].
 */
class Range {
 public:
  /*** ctor/dtor ***/
  Range();
  Range(const unsigned& start, const unsigned& end);
  Range(const unsigned& pos);

  /*** getter/setter ***/
  const unsigned& start() const;
  void start(const unsigned& start);
  const unsigned& end() const;
  void end(const unsigned& end);

};

/*!\class Loader
 * \brief Loader for LogicArray. LogicArray uses this class to load a block of
 * data into local storage unit.
 */
class Loader {
 public:
  virtual void load(void*, std::vector<Range>) = 0;
};

/*!\class DefaltLoader
 * \brief DefaultLoader simply load from a temporary file, create it if needed.
 */
class DefaultLoader : public Loader {
 public:
  virtual void load(void*, std::vector<Range>);
};

/*!\class Dumper
 * \brief Dumper for LogicArray. LogicArray uses this class to dump a block of
 * data from local storage unit to disk.
 */
class Dumper {
  virtual void dump(std::vector<Range>) = 0;
};

/*!\class DefaltDumper
 * \brief DefaultLoader write a block to a temporary file
 */
class DefaultLoader : public Loader {
  virtual void dump(std::vector<Range>);
};

/*!\class BlockManager
 * \brief Manage logic array blocks, check if it is in local storage, combine
 * blocks if necessary
 */
class BlockManager {
 public:
  /** ctor/dtor ***/
  BlockManager(unsigned num_dims);

  /*** member fucntions ***/
  /* getter/setter */
  const size_t& capacity() const;
  void capacity(const size_t& c);

  /* isLocal() */
  bool isLocal(std::vector<Range>);

  /* combine()
   * Combine ranges in the list to get a continuous address space for the
   * blocks.
   */
  void combine();

  /* diff()
   * Get a diff between \param Range and local so that we can bring a new block
   * having no intersection with the local ones.
   */
  std::vector<Range> diff(std::vector<Range>);

 private:
  std::list<Range> m_localblocks;
  unsigned m_numdims;
  size_t m_capacity;
};

/*!\class LogicArray
 * \brief LogicArray to represent a large chunk of data. 
 */
class LogicArray {
 public:
  /*** ctor/dtor ***/
  LogicArray(const unsigned& num_dims, 
             const std::vector<unsigned>& dim_sizes, 
             const unsigned& sizeof_elem, 
             const Loader& loader, 
             const Dumper& dumper);

  /*** member function ***/

  /* operator()()
   * Get/Set an element
   * If the block is not in local storage, load it into local using load(), a
   * range that is not overlaped by the needed block will be dumped using dump()
   */
  const void* operator()(std::vector<Range>) const;
  void* operator()(std::vector<Range>);

  /* load()
   * Explicitly load a block using m_loader. A space with the size large enough
   * to hold the block will be created, if size not enough, some of the blocks
   * will be dumped.
   */
  void load(std::vector<Range>);

  /* dump()
   * Explicitly dump a block using m_dumper.
   */
  void dump(std::vector<Range>);

 private:
  unsigned m_numdims;
  unsigned m_dimsizes;
  Loader& m_loader;       //use reference for polymorphism
  Dumper& m_dumper;       //use reference for polymorphism
  BlockManager* m_blkmnger;
};

/*!\class LogicArrayBlock
 * \brief A block of the logic array. This is just a wrapper to enable easier
 * interface.
 */
class LogicArrayBlock {
 public:
  /*** ctor/dtor ***/
  LogicArrayBlock(const LogicArray& array,
                  const std::vector<Range>& ranges);

  /*** member function ***/
  /* getter/setter */
  const LogicArray& array() const;
  const std::vector<Range>& ranges() const;

  /* load()
   * A wrapper of LogicArray load()
   */
  void load();

  /* dump()
   * A wrapper of LogicArray dump()
   */
  void dump();

 private:
  LogicArray* m_array;
  std::vector<Range> m_ranges;
}

#endif /* LOGICARRAY_H_ */
