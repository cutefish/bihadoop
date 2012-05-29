#ifndef INCLUDE_LOGICARRAY_H_
#define INCLUDE_LOGICARRAY_H_

#include <vector>

#include "Storage.h"
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

static const int EntireDim = -1;

} /* logic_array */

/*!\class Range
 * \brief Represent an continuous interval [start(), end()], including both start
 * and end.
 */
class Range {
 public:
  /*** ctor/dtor ***/
  Range(const unsigned& start, const unsigned& span);
  Range(const unsigned& pos);

  /*** getter/setter ***/
  const unsigned& start() const;
  void start(const unsigned& start);
  const unsigned& end() const;
  const unsigned& span() const;
  void span(const unsigned& span);

  /*** member function ***/

  /* hasOverlap()
   * Return true if self have intersection with other.
   */
  bool hasOverlapWith(const Range& other);

  /* contains()
   * Return true if self contains other.
   */
  bool contains(const Range& other);

  /* intersect()
   * Return [max(other.start, self.start), min(other.end, self.end)].
   * The validity of this operation should be checked before call.
   * if there is no overlap, this operation is not valid.
   */
  Range intersect(const Range& other);

  /* include()
   * Return [min(other.start, self.start), max(other.end, self.end)]
   * The validity of this operation should be checked before call.
   * if there is no overlap, this operation is not valid.
   */
  Range include(const Range& other);

  /* exclude()
   * Return the range not in other, but in self. 
   * The validity of this operation should be checked before call.
   * if there is an inclusion relationship, this operation is also not valid.
   */
  Range exclude(const Range& other);

 private:
  int m_start;
  int m_end;
};

class LogicArray;

/*!\class Loader
 * \brief Loader for LogicArray. Each Loader object belongs to only one
 * LogicArray object and that object uses the loader to load a block of data
 * into local storage unit. \p local is a local pointer to put the block of
 * data. The pointer is already prepared so that the size is enough.
 */
class Loader {
 public:
  virtual void load(StorageHandler local, 
                    StorageHandler std::vector<Range> block_range) = 0;
};

/*!\class DefaltLoader
 * \brief DefaultLoader simply created a temporary file.
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
 * \brief A block of the logic array. An interface to calculate the overlap
 * relationships.
 */
class LogicArrayBlock {
 public:
  /*** ctor/dtor ***/
  LogicArrayBlock();
  LogicArrayBlock(const LogicArray& array,
                  const std::vector<Range>& ranges);

  /*** member function ***/
  /* getter/setter */
  const LogicArray& array() const;
  const std::vector<Range>& ranges() const;
  size_t size() const;

  /* hasOverlap()
   * Return true if self have intersection with other.
   */
  bool hasOverlapWith(const LogicArrayBlock& other);

  /* contains()
   * Return true if self contains other.
   */
  bool contains(const LogicArrayBlock& other);

  /* diff()
   * Compare this block with other.
   * Return a vector of diff blocks and store the diff size(negative if
   * smaller).
   */
  std::vector<LogicArrayBlock> diff(const LogicArrayBlock& other, 
                                    size_t& diff_size);

  /* expand()
   */

  /* load()
   * A wrapper of LogicArray load()
   */
  void load();

  /* dump()
   * A wrapper of LogicArray dump()
   */
  void dump();

 private:
  /*** member variables ***/
  LogicArray* m_array;
  std::vector<Range> m_ranges;

  /*** private funcitons ***/
  /* intersect()
   * Return the intersection of self and other.
   */
  LogicArrayBlock intersect(const LogicArrayBlock& other);

}

#endif /* INCLUDE_LOGICARRAY_H_ */
